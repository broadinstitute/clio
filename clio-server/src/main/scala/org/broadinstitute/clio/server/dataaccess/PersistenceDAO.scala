package org.broadinstitute.clio.server.dataaccess

import org.broadinstitute.clio.server.ClioServerConfig.Persistence
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ClioDocument,
  ElasticsearchIndex
}

import com.sksamuel.elastic4s.Indexable
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.{ExecutionContext, Future}

import java.nio.file.{Files, Path}
import java.time.OffsetDateTime

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
      val writePath = Files.createDirectories(currentPathForIndex(index))
      val written = Files.write(
        writePath.resolve(s"${document.clioId}.json"),
        indexable.json(document).getBytes
      )
      logger.debug(s"Wrote document $document to ${written.toUri}")
    }

  private[dataaccess] def currentPathForIndex(
    index: ElasticsearchIndex[_]
  ): Path = {
    val updateTime = OffsetDateTime.now()
    rootPath.resolve(
      s"${index.rootDir}/${updateTime.getYear}/${updateTime.getMonthValue}/${updateTime.getDayOfMonth}"
    )
  }
}

object PersistenceDAO {
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
