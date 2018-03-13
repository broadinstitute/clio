package org.broadinstitute.clio.server.dataaccess

import java.nio.ByteBuffer
import java.nio.file.{Path, StandardOpenOption}
import java.time.OffsetDateTime

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.alpakka.file.scaladsl.Directory
import akka.stream.scaladsl.{Sink, Source}
import better.files.File
import com.typesafe.scalalogging.LazyLogging
import io.circe.Json
import io.circe.jawn.JawnParser
import org.broadinstitute.clio.server.ClioServerConfig.Persistence
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.UpsertId

import scala.concurrent.{ExecutionContext, Future}

/**
  * Persists metadata updates to a source of truth, allowing
  * for playback during disaster recovery.
  */
abstract class PersistenceDAO(recoveryParallelism: Int) extends LazyLogging {

  def this() = this(recoveryParallelism = 1)

  import PersistenceDAO.{StorageWalkDepth, VersionFileName}

  /**
    * Root path for all metadata writes.
    *
    * Using java.nio, this could be a local path or a
    * path to a cloud object in GCS.
    */
  def rootPath: File

  /**
    * Initialize the root storage directories for Clio documents.
    */
  def initialize(indexes: Seq[ElasticsearchIndex[_]], version: String)(
    implicit ec: ExecutionContext
  ): Future[Unit] = Future {
    // Make sure the rootPath can actually be used.
    val versionPath = rootPath / VersionFileName
    try {
      val versionFile = versionPath.write(version)
      logger.debug(s"Wrote current Clio version to ${versionFile.uri}")
    } catch {
      case e: Exception => {
        throw new RuntimeException(
          s"Couldn't write Clio version to ${versionPath.uri}, aborting!",
          e
        )
      }
    }

    // Since we log out the URI, the message will indicate if we're writing
    // to local disk vs. to cloud storage.
    logger.info(s"Initializing persistence storage at ${rootPath.uri}")
    indexes.foreach { index =>
      val path = (rootPath / index.rootDir).createDirectories()
      logger.debug(s"  ${index.indexName} -> ${path.uri}")
    }
  }

  /**
    * Write a metadata update to storage.
    *
    * @param document A JSON object representing an upsert.
    * @param index    Typeclass providing information on where to persist the
    *                 metadata update.
    */
  def writeUpdate(
    document: Json,
    index: ElasticsearchIndex[_],
    dt: OffsetDateTime = OffsetDateTime.now()
  )(
    implicit ec: ExecutionContext
  ): Future[Unit] = Future {
    val writePath = (rootPath / index.persistenceDirForDatetime(dt)).createDirectories()

    val upsertId = ElasticsearchIndex.getUpsertId(document)

    val jsonString = ModelAutoDerivation.defaultPrinter.pretty(document)

    val written = (writePath / upsertId.persistenceFilename)
      .write(jsonString)(Seq(StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE))
    logger.debug(s"Wrote document $document to ${written.uri}")
  }

  /**
    * Build a stream of all the paths to Clio upserts persisted under `rootDir`
    * under the given ordering, filtered by day-directory.
    */
  private def getPathsOrderedBy(
    rootDir: File,
    ordering: Ordering[Path],
    dayDirectoryFilter: Path => Boolean
  )(implicit ex: ExecutionContext, mat: Materializer): Source[File, NotUsed] = {
    val absoluteRoot = rootDir.path.toAbsolutePath

    // Walk the file tree to get all paths for the day-level directories.
    val sortedDirs = Directory
      .walk(absoluteRoot, maxDepth = Some(StorageWalkDepth))
      // Filter out month- and year-level directories.
      .filter { path =>
        /*
         * The google-cloud-nio adapter requires that the two sides of a
         * `relativize` call have the same return value for `isAbsolute()`:
         *
         * https://github.com/GoogleCloudPlatform/google-cloud-java/blob/master/google-cloud-contrib/google-cloud-nio/src/main/java/com/google/cloud/storage/contrib/nio/UnixPath.java#L345
         */
        path.toAbsolutePath
          .relativize(absoluteRoot)
          .getNameCount == StorageWalkDepth &&
        dayDirectoryFilter(path)
      }
      // Collect into a strict collection for sorting.
      .runWith(Sink.seq)
      .map(_.sorted(ordering))

    // Rebuild a stream from the sorted Seq.
    Source
      .fromFuture(sortedDirs)
      .mapConcat(identity)
      /*
       * We use `mapAsync` then `mapConcat`, instead of just `flatMapConcat`,
       * so we can sort the intermediate Seq result of the ls.
       */
      .mapAsync(recoveryParallelism)(Directory.ls(_).runWith(Sink.seq))
      .mapConcat(_.sorted(ordering))
      .map(File(_))
  }

  private val pathOrdering: Ordering[Path] = Ordering.by(_.toString)

  /**
    * Return the `ClioDocument`s stored under `rootDir` that were
    * written more recently than `lastToIgnore` (or all documents
    * if `lastToIgnore` not defined).
    *
    * @param rootDir is path to a bucket of "params files"
    * @param lastToIgnore the last (most recent) path in storage
    *                     that should be dropped instead of read
    *                     and converted into a document
    * @return the `ClioDocument` at `withId` and all later ones
    */
  private def getAllAfter(rootDir: File, lastToIgnore: Option[File])(
    implicit ec: ExecutionContext,
    materializer: Materializer
  ): Source[Json, NotUsed] = {

    val dayFilter = lastToIgnore.fold((_: Path) => true) { last =>
      val dayDirectoryOfLastUpsert = last.parent.toString
      /*
       * Given the path in storage to the last upsert known to Elasticsearch, we want to keep
       * only the day-directory in which we find the upsert, and all following directories.
       */
      _.toString >= dayDirectoryOfLastUpsert
    }

    // "not later" instead of "earlier" because will also match the last-known upsert itself.
    val docIsNotLaterThanLastUpsert = lastToIgnore.fold((_: File) => false) { last =>
      val pathOfLastUpsert = last.toString
      _.toString <= pathOfLastUpsert
    }

    val parser = new JawnParser
    /*
     * `dropWhile` instead of `filterNot` because `getPathsOrderedBy` returns the paths sorted
     * from earliest to latest, so we can stop testing paths after finding the first one more
     * recent than the latest upsert.
     */
    getPathsOrderedBy(rootDir, pathOrdering, dayFilter)
      .dropWhile(docIsNotLaterThanLastUpsert)
      .mapAsync(recoveryParallelism)(p => Future(p.byteArray))
      /*
       * By default, when an Akka stream is materialized all of its stages are "fused" into
       * a sequential pipeline, and only one stage is executed at a time. `.async` splits the
       * stream into two pieces which will run concurrently.
       *
       * In this stream, the use of `.async` means that as soon as the downstream decoding stage
       * pulls a ByteString from a file-to-recover, the above file-reading stage will immediately
       * pull for the next path to read. Without `.async`, the file-reading stage wouldn't try to
       * pull the next path until after all of its downstream stages finish executing.
       *
       * For more info see: https://doc.akka.io/docs/akka/current/stream/stream-parallelism.html#pipelining
       */
      .async
      .map(b => parser.parseByteBuffer(ByteBuffer.wrap(b)).fold(throw _, identity))
  }

  /**
    * Return the GCS bucket metadata files for `index` upserted on or
    * after `document`.  Decode the files from JSON as `ClioDocument`s
    * sorted by `upsertId`.
    *
    * @param mostRecentUpsert the ID of the last known upserted document,
    *                         `None` if all documents should be restored
    * @param index  to query against
    * @return a Future Seq of ClioDocument
    */
  def getAllSince(mostRecentUpsert: Option[UpsertId])(
    implicit ec: ExecutionContext,
    materializer: Materializer,
    index: ElasticsearchIndex[_]
  ): Source[Json, NotUsed] = {
    val rootDir = rootPath / index.rootDir

    mostRecentUpsert.fold(
      // If Elasticsearch contained no documents, load every JSON file in storage.
      getAllAfter(rootDir, None)
    ) { upsert =>
      val filename = upsert.persistenceFilename

      /*
       * Pull the stream until we find the path corresponding to the last
       * ID in Elasticsearch, remembering the last-seen Path. Since we take
       * inclusively, eventually the stream will emit the Path pointing to
       * the record for the most recent document in Elasticsearch.
       *
       * If the document isn't found, the stream will fail.
       */
      getPathsOrderedBy(rootDir, pathOrdering.reverse, _ => true)
        .takeWhile(_.name.toString != filename, inclusive = true)
        .fold[Option[File]](None)((_, p) => Some(p))
        .flatMapConcat { maybePathToLast =>
          maybePathToLast.fold(
            Source.failed[Json](
              new NoSuchElementException(
                s"No document found in storage for ID ${upsert.id}"
              )
            )
          ) { pathToLast =>
            logger.info(s"Found record for upsert ${upsert.id} at $pathToLast")
            getAllAfter(rootDir, Some(pathToLast))
          }
        }
    }
  }
}

object PersistenceDAO {

  /**
    * Name of the dummy file that Clio will write to the root of its configured
    * storage directory at startup, to ensure the given config is good.
    *
    * We have to write a dummy file for GCS because the google-cloud-nio adapter
    * assumes all directories exist and are writeable by design, since directories
    * aren't really a thing in GCS. We could special-case the local-file DAO but
    * the extra complexity isn't worth it.
    */
  val VersionFileName = "current-clio-version.txt"

  /**
    * Directory depth to naively walk during recovery.
    *
    * We store documents in directories binned by yyyy/MM/dd, so 3 is deep enough to
    * cover date-based partitions of documents in storage, but not the documents themselves.
    *
    * @see [[ElasticsearchIndex.dateTimeFormatter]]
    */
  val StorageWalkDepth = 3

  def apply(config: Persistence.PersistenceConfig, parallelism: Int): PersistenceDAO =
    config match {
      case l: Persistence.LocalConfig => new LocalFilePersistenceDAO(l, parallelism)
      case g: Persistence.GcsConfig   => new GcsPersistenceDAO(g, parallelism)
    }
}
