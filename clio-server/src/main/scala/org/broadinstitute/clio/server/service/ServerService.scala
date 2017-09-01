package org.broadinstitute.clio.server.service

import com.typesafe.scalalogging.StrictLogging
import org.broadinstitute.clio.server.{ClioApp, ClioServerConfig}
import org.broadinstitute.clio.server.dataaccess.{
  HttpServerDAO,
  SearchDAO,
  ServerStatusDAO
}
import org.broadinstitute.clio.status.model.ServerStatusInfo

import scala.concurrent.{ExecutionContext, Future}

class ServerService private (
  serverStatusDAO: ServerStatusDAO,
  httpServerDAO: HttpServerDAO,
  searchDAO: SearchDAO
)(implicit executionContext: ExecutionContext)
    extends StrictLogging {

  /**
    * Kick off a startup, and return immediately.
    */
  def beginStartup(): Unit = {
    // Simple no-op for now, illustrating how credentials can be loaded.
    val serviceAccount = ClioServerConfig.Persistence.serviceAccount
    serviceAccount.fold {
      logger.warn(
        "No GCS service account given, using local filesystem for persistence"
      )
    } { account =>
      val credential = account.credentialForScopes(Seq.empty)
      logger.info(
        s"Using service account with email ${credential.getClientEmail}"
      )
    }

    val startupAttempt = startup()
    startupAttempt.failed foreach { exception =>
      logger.error(
        s"Server failed to startup due to ${exception.getMessage}",
        exception
      )
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
      _ <- searchDAO.initialize()
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
  def apply(
    app: ClioApp
  )(implicit executionContext: ExecutionContext): ServerService = {
    new ServerService(app.serverStatusDAO, app.httpServerDAO, app.searchDAO)
  }
}
