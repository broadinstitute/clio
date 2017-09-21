package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.ClioApp
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
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
)(implicit executionContext: ExecutionContext) {

  /**
    * Kick off a startup, initializing all DAOs.
    */
  def beginStartup(): Future[Unit] = {
    for {
      _ <- serverStatusDAO.setStatus(ServerStatusInfo.Starting)
      _ <- persistenceDAO.initialize(
        ElasticsearchIndex.WgsUbam,
        ElasticsearchIndex.Gvcf
      )
      _ <- searchDAO.initialize()
    } yield ()
  }

  /**
    * Bind a port, and mark the server as ready to receive messages.
    */
  def completeStartup(): Future[Unit] = {
    for {
      _ <- httpServerDAO.startup()
      _ <- serverStatusDAO.setStatus(ServerStatusInfo.Started)
    } yield ()
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
    new ServerService(
      app.serverStatusDAO,
      app.httpServerDAO,
      app.persistenceDAO,
      app.searchDAO
    )
  }
}
