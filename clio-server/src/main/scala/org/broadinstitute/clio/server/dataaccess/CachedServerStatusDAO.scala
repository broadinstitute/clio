package org.broadinstitute.clio.server.dataaccess

import org.broadinstitute.clio.model.ServerStatusInfo

import scala.concurrent.Future
import scala.util.Try

/**
  * Stores the server status in a synchronized mutable variable.
  */
class CachedServerStatusDAO private () extends ServerStatusDAO {
  private val mutex = new Object()
  private var currentStatus: ServerStatusInfo = ServerStatusInfo.NotStarted

  override def setStatus(status: ServerStatusInfo): Future[Unit] = {
    Future.fromTry {
      Try {
        mutex synchronized {
          import CachedServerStatusDAO.level
          if (level(currentStatus) < level(status))
            currentStatus = status
        }
      }
    }
  }

  override def getStatus: Future[ServerStatusInfo] = {
    Future.fromTry {
      Try {
        mutex synchronized currentStatus
      }
    }
  }
}

object CachedServerStatusDAO {
  def apply(): CachedServerStatusDAO = {
    new CachedServerStatusDAO()
  }

  private def level(status: ServerStatusInfo): Int = {
    status match {
      case ServerStatusInfo.NotStarted   => 0
      case ServerStatusInfo.Starting     => 1
      case ServerStatusInfo.Started      => 2
      case ServerStatusInfo.ShuttingDown => 3
      case ServerStatusInfo.ShutDown     => 4
      case unknown                       => throw new RuntimeException(s"unknown status $unknown")
    }
  }
}
