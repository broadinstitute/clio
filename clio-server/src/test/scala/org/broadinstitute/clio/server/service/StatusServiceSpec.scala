package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.dataaccess._
import org.broadinstitute.clio.status.model.{SearchStatus, StatusInfo, VersionInfo}
import org.scalatest.{AsyncFlatSpec, Matchers}

class StatusServiceSpec extends AsyncFlatSpec with Matchers {
  behavior of "StatusService"

  it should "getVersion" in {
    val statusService = new MockStatusService()
    for {
      version <- statusService.getVersion
      _ = version should be(VersionInfo(MockServerStatusDAO.VersionMock))
    } yield succeed
  }

  it should "getStatus" in {
    val statusService = new MockStatusService()
    for {
      status <- statusService.getStatus
      _ = status should be(
        StatusInfo(MockServerStatusDAO.StatusMock, SearchStatus.OK)
      )
    } yield succeed
  }
}
