package org.broadinstitute.clio.server.dataaccess

import org.broadinstitute.clio.status.model.ServerStatusInfo

import scala.concurrent.Future

class MemoryServerStatusDAO extends MockServerStatusDAO {
  var setCalls: Seq[ServerStatusInfo] = Seq.empty

  override def setStatus(status: ServerStatusInfo): Future[Unit] = {
    setCalls :+= status
    super.setStatus(status)
  }
}
