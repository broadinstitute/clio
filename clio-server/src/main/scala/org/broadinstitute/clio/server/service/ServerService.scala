package org.broadinstitute.clio.server.service

import akka.stream.Materializer
import com.sksamuel.elastic4s.{HitReader, Indexable}
import com.sksamuel.elastic4s.circe._
import com.typesafe.scalalogging.StrictLogging
import io.circe.Decoder
import org.broadinstitute.clio.server.ClioApp
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ClioDocument,
  Elastic4sAutoDerivation,
  ElasticsearchIndex
}
import org.broadinstitute.clio.server.dataaccess.{
  HttpServerDAO,
  PersistenceDAO,
  SearchDAO,
  ServerStatusDAO
}
import org.broadinstitute.clio.status.model.ServerStatusInfo

import scala.concurrent.{ExecutionContext, Future}

class ServerService private (
  serverStatusDAO: ServerStatusDAO,
  httpServerDAO: HttpServerDAO,
  persistenceDAO: PersistenceDAO,
  searchDAO: SearchDAO
)(implicit executionContext: ExecutionContext, mat: Materializer)
    extends StrictLogging {

  /**
    * Kick off a startup, initializing all DAOs
    * and recovering metadata from storage.
    *
    * Returns immediately.
    */
  def beginStartup(): Unit = {
    val startupAttempt = startup()
    startupAttempt.failed.foreach { exception =>
      logger.error(s"Server failed to start due to $exception", exception)
      shutdown()
    }
  }

  /**
    * Recover missing documents for a given Elasticsearch index
    * from the source of truth.
    *
    * Fails immediately if an update pulled from the source of
    * truth fails to be applied to the search index.
    *
    * Returns the number of updates pulled & applied on success.
    */
  private[service] def recoverMetadata[
    D <: ClioDocument: Indexable: HitReader: Decoder
  ](index: ElasticsearchIndex[D]): Future[Int] = {
    for {
      mostRecentDocument <- searchDAO.getMostRecentDocument(index)
      documents <- persistenceDAO.getAllSince(mostRecentDocument, index)
      _ <- documents.foldLeft(Future.successful(())) {
        case (accFuture, document) => {
          for {
            /*
             * Don't update metadata for the current document until
             * the accumulator Future (the result updating metadata
             * for the previous document in storage) completes,
             * ensuring the order of updates is preserved.
             */
            _ <- accFuture
            _ = logger.debug(
              s"Recovering document with ID ${document.upsertId}"
            )
            _ <- searchDAO.updateMetadata(document, index)
          } yield {
            ()
          }
        }
      }
    } yield {
      documents.length
    }
  }

  /**
    * Block until shutdown, within some finite limit.
    */
  def awaitShutdown(): Unit = {
    httpServerDAO.awaitShutdown()
  }

  /**
    * Block until shutdown.
    */
  def awaitShutdownInf(): Unit = {
    httpServerDAO.awaitShutdownInf()
  }

  /**
    * Signal a shutdown, but wait until the shutdown occurs.
    */
  def shutdownAndWait(): Unit = {
    shutdown()
    awaitShutdown()
  }

  private[service] def startup(): Future[Unit] = {
    import Elastic4sAutoDerivation._
    for {
      _ <- serverStatusDAO.setStatus(ServerStatusInfo.Starting)
      _ <- persistenceDAO.initialize(
        ElasticsearchIndex.WgsUbam,
        ElasticsearchIndex.Gvcf
      )
      _ <- searchDAO.initialize()
      _ = logger.info("Recovering metadata from storage...")
      Seq(recoveredUbamCount, recoveredGvcfCount) <- Future.sequence(
        Seq(
          recoverMetadata(ElasticsearchIndex.WgsUbam),
          recoverMetadata(ElasticsearchIndex.Gvcf)
        )
      )
      _ = logger.info(s"Recovered $recoveredUbamCount wgs-ubams from storage")
      _ = logger.info(s"Recovered $recoveredGvcfCount gvcfs from storage")
      _ <- httpServerDAO.startup()
      _ <- serverStatusDAO.setStatus(ServerStatusInfo.Started)
      _ = logger.info("Server started")
    } yield ()
  }

  private[service] def shutdown(): Future[Unit] = {
    serverStatusDAO.setStatus(ServerStatusInfo.ShuttingDown) transformWith {
      _ =>
        searchDAO.close() transformWith { _ =>
          httpServerDAO.shutdown() transformWith { _ =>
            serverStatusDAO.setStatus(ServerStatusInfo.ShutDown)
          }
        }
    }
  }
}

object ServerService {
  def apply(app: ClioApp)(implicit executionContext: ExecutionContext,
                          mat: Materializer): ServerService = {
    new ServerService(
      app.serverStatusDAO,
      app.httpServerDAO,
      app.persistenceDAO,
      app.searchDAO
    )
  }
}
