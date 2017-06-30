package org.broadinstitute.clio.dataaccess

import org.broadinstitute.clio.model.ServerStatusInfo

import scala.concurrent.Future

class MockServerStatusDAO extends ServerStatusDAO {
  override def setStatus(status: ServerStatusInfo): Future[Unit] =
    Future.successful(())

  override def getStatus: Future[ServerStatusInfo] =
    Future.successful(MockServerStatusDAO.StatusMock)
}

object MockServerStatusDAO {
  val StatusMock = ServerStatusInfo.Started
}
