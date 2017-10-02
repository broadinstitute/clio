package org.broadinstitute.clio.server.dataaccess

import java.nio.file.{Files, Path}

import akka.stream.Materializer
import akka.stream.alpakka.file.scaladsl.Directory
import akka.stream.scaladsl.Sink
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
import org.broadinstitute.clio.util.model.UpsertId

import scala.concurrent.{ExecutionContext, Future}

/**
  * Persists metadata updates to a source of truth, allowing
  * for playback during disaster recovery.
  */
trait PersistenceDAO extends LazyLogging {

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
    indexes: ElasticsearchIndex[_]*
  )(implicit ec: ExecutionContext): Future[Unit] = Future {
    // Make sure the rootPath can actually be used.
    val versionPath = rootPath.resolve(PersistenceDAO.versionFileName)
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
    * Return the `ClioDocument`s under `rootdir` upserted later
    * than `withId`.
    *
    * @param rootDir is path to a bucket of "params files"
    * @param shouldRestore is a predicate which should return true for
    *                      all documents that should be restored
    * @param ec is an `ExecutionContext`
    * @tparam D is the type of `ClioDocument` expected under `rootDir`
    * @return the `ClioDocument` at `withId` and all later ones
    */
  private def getAllMatching[D <: ClioDocument: Decoder](
    rootDir: Path,
    shouldRestore: Path => Boolean
  )(implicit ec: ExecutionContext, materializer: Materializer): Future[Seq[D]] = {
    val document = (p: Path) => {
      decode[D](new String(Files.readAllBytes(p))).fold(throw _, identity)
    }
    Directory
      .walk(rootDir)
      .filter(shouldRestore(_))
      .map(p => document(p))
      .runWith(Sink.seq)
      .map(_.sortBy(_.upsertId))
  }

  /**
    * Return the GCS bucket metadata files for `index` upserted on or
    * after `upsertId`.  Decode the files from JSON as `ClioDocument`s
    * sorted by `upsertId`.
    *
    * @param upsertId of the last known upserted document, `None` if
    *                 all documents should be restored
    * @param index  to query against.
    * @param ec is the implicit ExecutionContext
    * @tparam D is a ClioDocument type with a JSON Decoder
    * @return a Future Seq of ClioDocument
    */
  def getAllSince[D <: ClioDocument: Decoder](upsertId: Option[UpsertId],
                                              index: ElasticsearchIndex[D])(
    implicit ec: ExecutionContext,
    materializer: Materializer
  ): Future[Seq[D]] = {
    val rootDir = rootPath.resolve(index.rootDir)
    val isJson = (p: Path) => p.toString.endsWith(".json")

    upsertId.fold(
      // If Elasticsearch contained no documents, load every JSON file in storage.
      getAllMatching[D](rootDir, isJson)
    ) { upsertId =>
      logger.debug(s"Recovering all upserts since $upsertId")

      val suffix = s"/${ClioDocument.persistenceFilename(upsertId)}"
      val filesWithId = Directory
        .walk(rootDir)
        .filter(_.toString.endsWith(suffix))
        .runWith(Sink.seq)
      filesWithId.map {
        case Seq(path) => {
          getAllMatching[D](
            rootDir,
            (p: Path) => isJson(p) && p.compareTo(path) > 0
          )
        }
        case paths =>
          throw new RuntimeException(
            s"${paths.size} files end with $suffix in ${rootDir.toUri}"
          )
      }.flatten
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
  val versionFileName = "current-clio-version.txt"

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
