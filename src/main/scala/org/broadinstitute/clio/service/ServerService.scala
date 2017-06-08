package org.broadinstitute.clio.service

import akka.actor.ActorSystem
import akka.pattern._
import cats.instances.future._
import cats.syntax.functor._
import com.typesafe.scalalogging.StrictLogging
import org.broadinstitute.clio.ClioApp
import org.broadinstitute.clio.dataaccess.elasticsearch.ElasticsearchIndexDefiners
import org.broadinstitute.clio.dataaccess.{ElasticsearchDAO, HttpServerDAO, ServerStatusDAO}
import org.broadinstitute.clio.model.{ElasticsearchIndex, ServerStatusInfo}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class ServerService private(serverStatusDAO: ServerStatusDAO, httpServerDAO: HttpServerDAO,
                            elasticsearchDAO: ElasticsearchDAO)
                           (implicit ec: ExecutionContext, system: ActorSystem) extends StrictLogging {
  /**
    * Kick off a startup, and return immediately.
    */
  def beginStartup(): Unit = {
    val startupAttempt = startup()
    startupAttempt.failed foreach { exception =>
      logger.error(s"Server failed to startup due to ${exception.getMessage}", exception)
      shutdown()
    }
    ()
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
    for {
      _ <- serverStatusDAO.setStatus(ServerStatusInfo.Starting)
      _ <- waitForElasticsearchReady()
      _ <- createOrUpdateIndices()
      _ <- httpServerDAO.startup()
      _ <- serverStatusDAO.setStatus(ServerStatusInfo.Started)
      _ = logger.info("Server started")
    } yield ()
  }

  private[service] def shutdown(): Future[Unit] = {
    serverStatusDAO.setStatus(ServerStatusInfo.ShuttingDown) transformWith { _ =>
      elasticsearchDAO.close() transformWith { _ =>
        httpServerDAO.shutdown() transformWith { _ =>
          serverStatusDAO.setStatus(ServerStatusInfo.ShutDown)
        }
      }
    }
  }

  private[service] def waitForElasticsearchReady(): Future[Unit] = {
    def waitWithRetry(retryCount: Int): Future[Unit] = {
      elasticsearchDAO.isReady transformWith {
        case Success(true) => Future.successful(())
        case Success(false) if retryCount <= 0 =>
          for {
            status <- elasticsearchDAO.getClusterStatus
          } yield {
            logger.info("Elastic search not ready. Giving up.")
            throw new RuntimeException(s"Elasticsearch server was not ready for processing. Last status was: $status")
          }
        case Failure(exception) if retryCount <= 0 =>
          logger.error(s"Unable to get color due to exception ${exception.getMessage}. Giving up.", exception)
          Future.failed(exception)
        case Success(false) =>
          val waitDuration = elasticsearchDAO.readyPatience
          logger.info(s"Elasticsearch not ready. Will try again in $waitDuration.")
          after(waitDuration, system.scheduler)(waitWithRetry(retryCount - 1))
        case Failure(exception) =>
          val waitDuration = elasticsearchDAO.readyPatience
          logger.error(
            s"Unable to get Elasticsearch status due to exception ${exception.getMessage}. " +
              s"Will try again in $waitDuration.",
            exception)
          after(waitDuration, system.scheduler)(waitWithRetry(retryCount - 1))
      }
    }

    waitWithRetry(elasticsearchDAO.readyRetries)
  }

  private[service] def createOrUpdateIndices(): Future[Unit] = {
    def createOrUpdateIndex(index: ElasticsearchIndex): Future[Unit] = {
      for {
        exists <- elasticsearchDAO.existsIndexType(index)
        _ <- if (exists) Future.successful(()) else elasticsearchDAO.createIndexType(index)
        _ <- elasticsearchDAO.updateFieldDefinitions(index)
      } yield ()
    }

    Future.sequence(ElasticsearchIndexDefiners.All.map(definer => createOrUpdateIndex(definer.indexDefinition))).void
  }
}

object ServerService {
  def apply(app: ClioApp)(implicit ec: ExecutionContext, system: ActorSystem): ServerService = {
    new ServerService(app.serverStatusDAO, app.httpServerDAO, app.elasticsearchDAO)
  }
}
