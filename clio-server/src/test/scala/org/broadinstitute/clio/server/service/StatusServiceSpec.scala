package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.MockClioApp
import org.broadinstitute.clio.server.dataaccess.{
  MockHttpServerDAO,
  MockSearchDAO,
  MockServerStatusDAO
}
import org.broadinstitute.clio.server.model.{StatusInfo, VersionInfo}
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
      _ = status should be(
        StatusInfo(MockServerStatusDAO.StatusMock, MockSearchDAO.StatusMock)
      )
    } yield succeed
  }
}
