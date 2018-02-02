package org.broadinstitute.clio.server.dataaccess

import org.broadinstitute.clio.status.model.ClioStatus

import scala.concurrent.Future
import scala.util.Try

/**
  * Stores the server status in a synchronized mutable variable.
  */
class CachedServerStatusDAO private () extends ServerStatusDAO {
  private val mutex = new Object()
  private var currentStatus: ClioStatus = ClioStatus.NotStarted

  override def setStatus(status: ClioStatus): Future[Unit] = {
    Future.fromTry {
      Try {
        mutex synchronized {
          if (currentStatus.level < status.level)
            currentStatus = status
        }
      }
    }
  }

  override def getStatus: Future[ClioStatus] = {
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
}
