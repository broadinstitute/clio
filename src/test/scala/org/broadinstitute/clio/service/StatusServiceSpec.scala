package org.broadinstitute.clio.service

import org.broadinstitute.clio.MockClioApp
import org.broadinstitute.clio.dataaccess.{MockElasticsearchDAO, MockHttpServerDAO, MockServerStatusDAO}
import org.broadinstitute.clio.model.{StatusInfo, VersionInfo}
import org.scalatest.{AsyncFlatSpec, Matchers}

class StatusServiceSpec extends AsyncFlatSpec with Matchers {
  behavior of "StatusService"

  it should "getVersion" in {
    val app = MockClioApp()
    val statusService = StatusService(app)
    for {
      version <- statusService.getVersion
      _ = version should be(VersionInfo(MockHttpServerDAO.VersionMock))
    } yield succeed
  }

  it should "getStatus" in {
    val app = MockClioApp()
    val statusService = StatusService(app)
    for {
      status <- statusService.getStatus
      _ = status should be(StatusInfo(MockServerStatusDAO.StatusMock, MockElasticsearchDAO.StatusMock))
    } yield succeed
  }
}
