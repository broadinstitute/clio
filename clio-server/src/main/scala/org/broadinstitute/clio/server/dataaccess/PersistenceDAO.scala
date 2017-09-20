package org.broadinstitute.clio.server.dataaccess

import java.io.File

import org.broadinstitute.clio.server.ClioServerConfig
import org.broadinstitute.clio.server.ClioServerConfig.Persistence
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ClioDocument,
  ElasticsearchIndex
}
import com.sksamuel.elastic4s.Indexable
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.{ExecutionContext, Future}
import java.nio.file.{Files, Path}
import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import akka.stream.alpakka.file.scaladsl.Directory
import akka.stream.scaladsl.{Flow, Sink, Source}
import io.circe.Decoder
import io.circe.parser._

/**
  * Persists metadata updates to a source of truth, allowing
  * for playback during disaster recovery.
  */
trait PersistenceDAO extends LazyLogging {

  private implicit val system = ActorSystem("clio")

  private val loggingDecider: Supervision.Decider = { error =>
    logger.error("stopping due to error", error)
    Supervision.Stop
  }

  private lazy val actorMaterializerSettings =
    ActorMaterializerSettings(system).withSupervisionStrategy(loggingDecider)

  private implicit lazy val materializer = ActorMaterializer(
    actorMaterializerSettings
  )

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
        writePath.resolve(s"${document.upsertId}.json"),
        indexable.json(document).getBytes
      )
      logger.debug(s"Wrote document $document to ${written.toUri}")
    }

  def documentFromPath[D <: ClioDocument: Decoder](path: Path): D = {
    decode[D](new String(Files.readAllBytes(path))).fold(throw _, identity)
  }

  def isParamsFile(path: Path) = {
    val s = path.toString
    val parts = s.split(File.separator)
    val last = parts.last
    val isJson = last.endsWith(".json")
    val hasUuid = last.split('-').length == 5
    parts.length == 6 && isJson && hasUuid
  }

  def sinceId[D <: ClioDocument: Decoder](rootDir: Path, withId: Path)(
    implicit ec: ExecutionContext
  ): Future[Seq[D]] = {
    val before = (p: Path) => p.compareTo(withId) < 0
    val after = Directory
      .walk(rootDir)
      .filter(p => isParamsFile(p))
      .filter(!before(_))
      .runWith(Sink.seq)
      .map(_.sortBy(before))
    val result = Source
      .fromFuture(after)
      .mapConcat(identity)
      .via(Flow[Path].map(p => documentFromPath(p)))
      .runWith(Sink.seq[D])
    result
  }

  /**
    * Return the GCS bucket metadata files for `index` upserted on or
    * after `upsertId`.  Decode the files from JSON as `ClioDocument`s
    * sorted by `upsertId`.
    *
    * @param upsertId of the last known upserted document
    * @param index  to query against.
    * @param ec is the implicit ExecutionContext
    * @tparam D is a ClioDocument type with a JSON Decoder
    * @return a Future Seq of ClioDocument
    */
  def getAllSince[D <: ClioDocument: Decoder](
    upsertId: UUID,
    index: ElasticsearchIndex[D]
  )(implicit ec: ExecutionContext): Future[Seq[D]] = {
    val rootDir = rootPath.resolve(index.rootDir)
    val suffix = s"/${upsertId}.json"
    val filesWithId = Directory
      .walk(rootDir)
      .filter(p => isParamsFile(p))
      .filter(_.toString.endsWith(suffix))
      .limit(23)
      .runWith(Sink.seq)
    filesWithId
      .map(_.size)
      .map(n => {
        if (n != 1) {
          throw new RuntimeException(
            s"${n} files end with ${suffix} in ${rootDir.normalize.toString}"
          )
        }
      })
    filesWithId
      .map(_.map(sinceId(rootDir, _)))
      .map(Future.sequence(_))
      .flatMap(identity)
      .map(_.flatMap(identity))
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
