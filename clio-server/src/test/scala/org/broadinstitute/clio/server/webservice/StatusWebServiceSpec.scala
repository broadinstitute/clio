package org.broadinstitute.clio.server.webservice

import org.broadinstitute.clio.server.service.StatusService
import org.broadinstitute.clio.status.model.{
  ClioStatus,
  SearchStatus,
  StatusInfo,
  VersionInfo
}
import org.broadinstitute.clio.transfer.model.ApiConstants._
import org.scalamock.scalatest.MockFactory

import scala.concurrent.Future

class StatusWebServiceSpec extends BaseWebserviceSpec with MockFactory {
  behavior of "StatusWebService"

  val statusService: StatusService = mock[StatusService]
  val webService = new StatusWebService(statusService)

  it should "versionRoute" in {
    val expectedVersion = VersionInfo("Mock Server Version")
    (statusService.getVersion _).expects().returning(Future(expectedVersion))
    Get(s"/$versionString") ~> webService.versionRoute ~> check {
      responseAs[VersionInfo] should be(expectedVersion)
    }
  }

  it should "healthRoute" in {
    val expectedStatus = StatusInfo(ClioStatus.Started, SearchStatus.OK)
    (statusService.getStatus _).expects().returning(Future(expectedStatus))
    Get(s"/$healthString") ~> webService.healthRoute ~> check {
      responseAs[StatusInfo] should be(expectedStatus)
    }
  }
}
