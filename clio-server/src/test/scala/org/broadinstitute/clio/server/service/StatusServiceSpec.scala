package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.MockClioApp
import org.broadinstitute.clio.server.dataaccess.{MockHttpServerDAO, MockServerStatusDAO}
import org.broadinstitute.clio.status.model.{StatusInfo, SearchStatus, VersionInfo}

import org.scalatest.{AsyncFlatSpec, Matchers}

class StatusServiceSpec extends AsyncFlatSpec with Matchers {
  behavior of "StatusService"

  it should "getVersion" in {
    val app = MockClioApp()
    val httpServerDAO = new MockHttpServerDAO()
    val statusService = StatusService(app, httpServerDAO)
    for {
      version <- statusService.getVersion
      _ = version should be(VersionInfo(MockHttpServerDAO.VersionMock))
    } yield succeed
  }

  it should "getStatus" in {
    val app = MockClioApp()
    val httpServerDAO = new MockHttpServerDAO()
    val statusService = StatusService(app, httpServerDAO)
    for {
      status <- statusService.getStatus
      _ = status should be(
        StatusInfo(MockServerStatusDAO.StatusMock, SearchStatus.OK)
      )
    } yield succeed
  }
}
