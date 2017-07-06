package org.broadinstitute.clio.server.webservice

import akka.http.scaladsl.testkit.ScalatestRouteTest
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import io.circe.generic.auto._
import org.broadinstitute.clio.server.dataaccess.{
  MockSearchDAO,
  MockHttpServerDAO,
  MockServerStatusDAO
}
import org.broadinstitute.clio.model.{StatusInfo, VersionInfo}
import org.scalatest.{FlatSpec, Matchers}

class StatusWebServiceSpec
    extends FlatSpec
    with Matchers
    with ScalatestRouteTest {
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
        StatusInfo(MockServerStatusDAO.StatusMock, MockSearchDAO.StatusMock)
      )
    }
  }
}
