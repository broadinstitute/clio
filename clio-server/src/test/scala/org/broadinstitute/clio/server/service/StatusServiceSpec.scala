package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.dataaccess._
import org.broadinstitute.clio.status.model.{SearchStatus, StatusInfo, VersionInfo}
import org.scalatest.{AsyncFlatSpec, Matchers}

class StatusServiceSpec extends AsyncFlatSpec with Matchers {
  behavior of "StatusService"

  def statusServiceWithMockDefaults(
    statusDao: ServerStatusDAO = new MockServerStatusDAO(),
    searchDAO: SearchDAO = new MockSearchDAO(),
    httpServerDAO: HttpServerDAO = new MockHttpServerDAO
  ) = StatusService(
    statusDao,
    searchDAO,
    httpServerDAO
  )

  it should "getVersion" in {
    val statusService = statusServiceWithMockDefaults()
    for {
      version <- statusService.getVersion
      _ = version should be(VersionInfo(MockHttpServerDAO.VersionMock))
    } yield succeed
  }

  it should "getStatus" in {
    val statusService = statusServiceWithMockDefaults()
    for {
      status <- statusService.getStatus
      _ = status should be(
        StatusInfo(MockServerStatusDAO.StatusMock, SearchStatus.OK)
      )
    } yield succeed
  }
}
