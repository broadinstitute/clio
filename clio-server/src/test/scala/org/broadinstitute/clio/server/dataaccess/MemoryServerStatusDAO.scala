package org.broadinstitute.clio.server.dataaccess

import org.broadinstitute.clio.status.model.ServerStatusInfo

import scala.collection.mutable
import scala.concurrent.Future

class MemoryServerStatusDAO extends MockServerStatusDAO {
  val setCalls: mutable.ArrayBuffer[ServerStatusInfo] =
    mutable.ArrayBuffer.empty

  override def setStatus(status: ServerStatusInfo): Future[Unit] = {
    setCalls += status
    super.setStatus(status)
  }
}
