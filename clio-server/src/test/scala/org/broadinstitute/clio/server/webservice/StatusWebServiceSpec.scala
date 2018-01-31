package org.broadinstitute.clio.server.webservice

import org.broadinstitute.clio.server.dataaccess.{MockHttpServerDAO, MockServerStatusDAO}
import org.broadinstitute.clio.status.model.{StatusInfo, SearchStatus, VersionInfo}

class StatusWebServiceSpec extends BaseWebserviceSpec {
  behavior of "StatusWebService"

  it should "versionRoute" in {
    val webService = new MockStatusWebService()
    Get("/version") ~> webService.versionRoute ~> check {
      responseAs[VersionInfo] should be(
        VersionInfo(MockHttpServerDAO.VersionMock)
      )
    }
  }

  it should "healthRoute" in {
    val webService = new MockStatusWebService()
    Get("/health") ~> webService.healthRoute ~> check {
      responseAs[StatusInfo] should be(
        StatusInfo(MockServerStatusDAO.StatusMock, SearchStatus.OK)
      )
    }
  }
}
