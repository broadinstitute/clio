package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.dataaccess.{MockSearchDAO, MockServerStatusDAO}
import org.broadinstitute.clio.status.model.VersionInfo

import scala.concurrent.{ExecutionContext, Future}

class MockStatusService()(
  implicit executionContext: ExecutionContext
) extends StatusService(
      new MockServerStatusDAO(),
      new MockSearchDAO()
    ) {
  override def getVersion: Future[VersionInfo] = {
    Future.successful(
      VersionInfo(
        MockServerStatusDAO.VersionMock
      )
    )
  }
}
