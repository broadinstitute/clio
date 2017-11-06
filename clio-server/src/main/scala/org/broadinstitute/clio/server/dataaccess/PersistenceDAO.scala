package org.broadinstitute.clio.server.dataaccess

import java.nio.file.{Files, Path}

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.alpakka.file.scaladsl.Directory
import akka.stream.scaladsl.{Sink, Source}
import com.sksamuel.elastic4s.Indexable
import com.typesafe.scalalogging.LazyLogging
import io.circe.Decoder
import io.circe.parser._
import org.broadinstitute.clio.server.ClioServerConfig
import org.broadinstitute.clio.server.ClioServerConfig.Persistence
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ClioDocument,
  ElasticsearchIndex
}

import scala.concurrent.{ExecutionContext, Future}

/**
  * Persists metadata updates to a source of truth, allowing
  * for playback during disaster recovery.
  */
trait PersistenceDAO extends LazyLogging {

  import PersistenceDAO.{StorageWalkDepth, VersionFileName}

  /**
    * Root path for all metadata writes.
    *
    * Using java.nio, this could be a local path or a
    * path to a cloud object in GCS.
    */
  def rootPath: Path

  /**
    * Initialize the root storage directories for Clio documents.
    */
  def initialize(
    indexes: Seq[ElasticsearchIndex[_]]
  )(implicit ec: ExecutionContext): Future[Unit] = Future {
    // Make sure the rootPath can actually be used.
    val versionPath = rootPath.resolve(VersionFileName)
    try {
      val versionFile =
        Files.write(versionPath, ClioServerConfig.Version.value.getBytes)
      logger.debug(s"Wrote current Clio version to ${versionFile.toUri}")
    } catch {
      case e: Exception => {
        throw new RuntimeException(
          s"Couldn't write Clio version to ${versionPath.toUri}, aborting!",
          e
        )
      }
    }

    // Since we log out the URI, the message will indicate if we're writing
    // to local disk vs. to cloud storage.
    logger.info(s"Initializing persistence storage at ${rootPath.toUri}")
    indexes.foreach { index =>
      val path = Files.createDirectories(rootPath.resolve(index.rootDir))
      logger.debug(s"  ${index.indexName} -> ${path.toUri}")
    }
  }

  /**
    * Write a metadata update to storage.
    *
    * @param document A partial metadata document representing an upsert.
    * @param index    Typeclass providing information on where to persist the
    *                 metadata update.
    */
  def writeUpdate[D <: ClioDocument](
    document: D,
    index: ElasticsearchIndex[D]
  )(implicit ec: ExecutionContext, indexable: Indexable[D]): Future[Unit] =
    Future {
      val writePath =
        Files.createDirectories(rootPath.resolve(index.currentPersistenceDir))
      val written = Files.write(
        writePath.resolve(s"${document.persistenceFilename}"),
        indexable.json(document).getBytes
      )
      logger.debug(s"Wrote document $document to ${written.toUri}")
    }

  /**
    * Build a stream of all the paths to Clio upserts persisted under `rootDir`
    * under the given ordering, filtered by day-directory.
    */
  private def getPathsOrderedBy(
    rootDir: Path,
    ordering: Ordering[Path],
    dayDirectoryFilter: Path => Boolean
  )(implicit ex: ExecutionContext, mat: Materializer): Source[Path, NotUsed] = {
    val absoluteRoot = rootDir.toAbsolutePath

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
      .mapAsync(1)(Directory.ls(_).runWith(Sink.seq))
      .mapConcat(_.sorted(ordering))
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
    * @tparam D is the type of `ClioDocument` expected under `rootDir`
    * @return the `ClioDocument` at `withId` and all later ones
    */
  private def getAllAfter[D <: ClioDocument: Decoder](
    rootDir: Path,
    lastToIgnore: Option[Path]
  )(implicit ec: ExecutionContext,
    materializer: Materializer): Source[D, NotUsed] = {

    val dayFilter = lastToIgnore.fold((_: Path) => true) {
      /*
       * We filter inclusively on date because the most recent
       * document in Elasticsearch might not have been the last
       * document upsert-ed during the day it was added.
       */
      last => (dir: Path) =>
        last.getParent.toString >= dir.toString
    }

    val docFilter = lastToIgnore.fold((_: Path) => false) {
      last => (path: Path) =>
        last.toString >= path.toString
    }

    getPathsOrderedBy(rootDir, pathOrdering, dayFilter)
      .dropWhile(docFilter)
      .map { p =>
        decode[D](new String(Files.readAllBytes(p))).fold(throw _, identity)
      }
  }

  /**
    * Return the GCS bucket metadata files for `index` upserted on or
    * after `document`.  Decode the files from JSON as `ClioDocument`s
    * sorted by `upsertId`.
    *
    * @param mostRecentDocument the last known upserted document, `None`
    *                           if all documents should be restored
    * @param index  to query against
    * @tparam D is a ClioDocument type with a JSON Decoder
    * @return a Future Seq of ClioDocument
    */
  def getAllSince[D <: ClioDocument: Decoder](
    mostRecentDocument: Option[ClioDocument],
    index: ElasticsearchIndex[D]
  )(implicit ec: ExecutionContext,
    materializer: Materializer): Source[D, NotUsed] = {
    val rootDir = rootPath.resolve(index.rootDir)

    mostRecentDocument.fold(
      // If Elasticsearch contained no documents, load every JSON file in storage.
      getAllAfter(rootDir, None)
    ) { document =>
      logger.debug(
        s"Recovering all upserts from index ${index.indexName} since ${document.upsertId}"
      )
      val filename = document.persistenceFilename

      /*
       * Pull the stream until we find the path corresponding to the last
       * ID in Elasticsearch, remembering the last-seen Path. Since we take
       * inclusively, eventually the stream will emit the Path pointing to
       * the record for the most recent document in Elasticsearch.
       *
       * If the document isn't found, the stream will fail.
       */
      getPathsOrderedBy(rootDir, pathOrdering.reverse, _ => true)
        .takeWhile(_.getFileName.toString != filename, inclusive = true)
        .fold[Option[Path]](None)((_, p) => Some(p))
        .flatMapConcat { maybePathToLast =>
          maybePathToLast.fold(
            Source.failed[D](
              new NoSuchElementException(
                s"No document found in storage for ID ${document.upsertId}"
              )
            )
          )(pathToLast => getAllAfter(rootDir, Some(pathToLast)))
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

  def apply(config: Persistence.PersistenceConfig): PersistenceDAO =
    config match {
      case l: Persistence.LocalConfig => {
        new LocalFilePersistenceDAO(l)
      }
      case g: Persistence.GcsConfig => {
        new GcsPersistenceDAO(g)
      }
    }
}
