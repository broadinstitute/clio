package org.broadinstitute.clio.server.dataaccess

import org.broadinstitute.clio.status.model.ClioStatus

import scala.collection.mutable
import scala.concurrent.Future

class MemoryServerStatusDAO extends MockServerStatusDAO {

  val setCalls: mutable.ArrayBuffer[ClioStatus] =
    mutable.ArrayBuffer.empty

  override def setStatus(status: ClioStatus): Future[Unit] = {
    setCalls += status
    super.setStatus(status)
  }
}
