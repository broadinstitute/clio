package org.broadinstitute.clio.webservice

import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import io.circe.generic.auto._
import org.broadinstitute.clio.dataaccess.MockHttpServerDAO
import org.broadinstitute.clio.model.{StatusInfo, VersionInfo}
import org.scalatest.{FlatSpec, Matchers}

class StatusWebServiceSpec extends FlatSpec with Matchers with ClioWebServiceSpec with StatusWebService {

  behavior of "StatusWebService"

  it should "versionRoute" in {
    Get("/version") ~> versionRoute ~> check {
      responseAs[VersionInfo] should be(VersionInfo(MockHttpServerDAO.VersionMock))
    }
  }

  it should "healthRoute" in {
    Get("/health") ~> healthRoute ~> check {
      responseAs[StatusInfo] should be(StatusInfo(MockHttpServerDAO.StatusMock))
    }
  }
}
