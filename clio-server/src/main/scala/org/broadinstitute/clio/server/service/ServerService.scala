package org.broadinstitute.clio.server.service

import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.typesafe.scalalogging.StrictLogging
import io.circe.Decoder
import org.broadinstitute.clio.server.{ClioApp, ClioServerConfig}
import org.broadinstitute.clio.server.dataaccess.elasticsearch._
import org.broadinstitute.clio.server.dataaccess.{
  HttpServerDAO,
  PersistenceDAO,
  SearchDAO,
  ServerStatusDAO
}
import org.broadinstitute.clio.status.model.ClioStatus
import org.broadinstitute.clio.util.json.ModelAutoDerivation

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

class ServerService private (
  serverStatusDAO: ServerStatusDAO,
  httpServerDAO: HttpServerDAO,
  persistenceDAO: PersistenceDAO,
  searchDAO: SearchDAO,
  version: String
)(implicit executionContext: ExecutionContext, mat: Materializer)
    extends ModelAutoDerivation
    with StrictLogging {

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
    D <: ClioDocument: Decoder
  ]()(implicit index: ElasticsearchIndex[D]): Future[Int] = {
    searchDAO.getMostRecentDocument.flatMap { mostRecent =>
      val msg = s"Recovering all upserts from index ${index.indexName}"
      logger.info(mostRecent.fold(msg)(d => s"$msg since ${d.upsertId}"))

      persistenceDAO
        .getAllSince[D](mostRecent)
        .runWith(Sink.foldAsync(0) { (count, doc) =>
          logger.debug(s"Recovering document with ID ${doc.upsertId}")
          searchDAO.updateMetadata(doc).map(_ => count + 1)
        })
        .andThen {
          case Success(count) =>
            logger.info(s"Recovered $count upserts for index ${index.indexName}")
        }
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

    val indexes = immutable.Seq(
      ElasticsearchIndex[DocumentWgsUbam],
      ElasticsearchIndex[DocumentGvcf],
      ElasticsearchIndex[DocumentWgsCram]
    )

    for {
      _ <- serverStatusDAO.setStatus(ClioStatus.Starting)
      _ <- persistenceDAO.initialize(indexes, version)
      _ <- searchDAO.initialize(indexes)
      _ <- httpServerDAO.startup()
      _ <- serverStatusDAO.setStatus(ClioStatus.Recovering)
      _ = logger.info("Recovering metadata from storage...")
      _ <- Future.sequence(
        Seq(
          recoverMetadata[DocumentWgsUbam](),
          recoverMetadata[DocumentGvcf](),
          recoverMetadata[DocumentWgsCram]()
        )
      )
      _ <- httpServerDAO.enableApi()
      _ <- serverStatusDAO.setStatus(ClioStatus.Started)
      _ = logger.info("Server started")
    } yield ()
  }

  private[service] def shutdown(): Future[Unit] = {
    serverStatusDAO.setStatus(ClioStatus.ShuttingDown) transformWith { _ =>
      searchDAO.close() transformWith { _ =>
        httpServerDAO.shutdown() transformWith { _ =>
          serverStatusDAO.setStatus(ClioStatus.ShutDown)
        }
      }
    }
  }
}

object ServerService {

  def apply(
    app: ClioApp
  )(implicit executionContext: ExecutionContext, mat: Materializer): ServerService = {
    new ServerService(
      app.serverStatusDAO,
      app.httpServerDAO,
      app.persistenceDAO,
      app.searchDAO,
      ClioServerConfig.Version.value
    )
  }
}
