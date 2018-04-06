package org.broadinstitute.clio.server.webservice

import org.broadinstitute.clio.server.dataaccess.MockServerStatusDAO
import org.broadinstitute.clio.status.model.{SearchStatus, StatusInfo, VersionInfo}
import org.broadinstitute.clio.transfer.model.ApiConstants._

class StatusWebServiceSpec extends BaseWebserviceSpec {
  behavior of "StatusWebService"

  it should "versionRoute" in {
    val webService = new MockStatusWebService()
    Get(s"/$versionString") ~> webService.versionRoute ~> check {
      responseAs[VersionInfo] should be(
        VersionInfo(MockServerStatusDAO.VersionMock)
      )
    }
  }

  it should "healthRoute" in {
    val webService = new MockStatusWebService()
    Get(s"/$healthString") ~> webService.healthRoute ~> check {
      responseAs[StatusInfo] should be(
        StatusInfo(MockServerStatusDAO.StatusMock, SearchStatus.OK)
      )
    }
  }
}
