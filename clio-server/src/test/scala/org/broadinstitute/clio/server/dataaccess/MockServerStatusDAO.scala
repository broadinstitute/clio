package org.broadinstitute.clio.server.dataaccess

import org.broadinstitute.clio.status.model.ClioStatus

import scala.concurrent.Future

class MockServerStatusDAO extends ServerStatusDAO {
  override def setStatus(status: ClioStatus): Future[Unit] =
    Future.successful(())

  override def getStatus: Future[ClioStatus] =
    Future.successful(MockServerStatusDAO.StatusMock)
}

object MockServerStatusDAO {
  val StatusMock = ClioStatus.Started
}
