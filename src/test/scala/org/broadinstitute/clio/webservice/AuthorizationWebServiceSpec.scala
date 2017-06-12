package org.broadinstitute.clio.webservice

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.http.scaladsl.server.AuthorizationFailedRejection
import org.scalatest.{FlatSpec, Matchers}
import org.broadinstitute.clio.model._
import org.broadinstitute.clio.service._

import scala.concurrent.ExecutionContext



class MockAuthorizationWebService()(implicit ec: ExecutionContext) extends AuthorizationWebService {
  override lazy val authorizationService: AuthorizationService = AuthorizationService()
}


class AuthorizationWebServiceSpec extends FlatSpec
    with Matchers with ScalatestRouteTest {

  behavior of "AuthorizationWebService"

  it should "reject missing token header" in {
    val webService = new MockAuthorizationWebService()
    Get("/authorization").
      addHeader(OidcClaimExpiresIn(AuthorizationService.mock.expires)).
      addHeader(OidcClaimEmail(AuthorizationService.mock.email)).
      addHeader(OidcClaimSub(AuthorizationService.mock.id)) ~>
    webService.authorizationRoute ~> check {
      rejection shouldEqual AuthorizationFailedRejection
    }
  }

  it should "reject missing expires header" in {
    val webService = new MockAuthorizationWebService()
    Get("/authorization").
      addHeader(OidcAccessToken(AuthorizationService.mock.token)).
      addHeader(OidcClaimEmail(AuthorizationService.mock.email)).
      addHeader(OidcClaimSub(AuthorizationService.mock.id)) ~>
    webService.authorizationRoute ~> check {
      rejection shouldEqual AuthorizationFailedRejection
    }
  }

  it should "reject missing email header" in {
    val webService = new MockAuthorizationWebService()
    Get("/authorization").
      addHeader(OidcAccessToken(AuthorizationService.mock.token)).
      addHeader(OidcClaimExpiresIn(AuthorizationService.mock.expires)).
      addHeader(OidcClaimSub(AuthorizationService.mock.id)) ~>
    webService.authorizationRoute ~> check {
      rejection shouldEqual AuthorizationFailedRejection
    }
  }

  it should "reject missing id headers" in {
    val webService = new MockAuthorizationWebService()
    Get("/authorization").
      addHeader(OidcAccessToken(AuthorizationService.mock.token)).
      addHeader(OidcClaimExpiresIn(AuthorizationService.mock.expires)).
      addHeader(OidcClaimEmail(AuthorizationService.mock.email)) ~>
    webService.authorizationRoute ~> check {
      rejection shouldEqual AuthorizationFailedRejection
    }
  }

  it should "accept with subject ID header" in {
    val webService = new MockAuthorizationWebService()
    Get("/authorization").
      addHeader(OidcAccessToken(AuthorizationService.mock.token)).
      addHeader(OidcClaimExpiresIn(AuthorizationService.mock.expires)).
      addHeader(OidcClaimEmail(AuthorizationService.mock.email)).
      addHeader(OidcClaimSub(AuthorizationService.mock.id)) ~>
    webService.authorizationRoute ~> check {
      status shouldEqual StatusCodes.OK
    }
  }

  it should "accept with user ID header" in {
    val webService = new MockAuthorizationWebService()
    Get("/authorization").
      addHeader(OidcAccessToken(AuthorizationService.mock.token)).
      addHeader(OidcClaimExpiresIn(AuthorizationService.mock.expires)).
      addHeader(OidcClaimEmail(AuthorizationService.mock.email)).
      addHeader(OidcClaimUserId(AuthorizationService.mock.id)) ~>
    webService.authorizationRoute ~> check {
      status shouldEqual StatusCodes.OK
    }
  }

  it should "accept with both ID headers" in {
    val webService = new MockAuthorizationWebService()
    Get("/authorization").
      addHeader(OidcAccessToken(AuthorizationService.mock.token)).
      addHeader(OidcClaimExpiresIn(AuthorizationService.mock.expires)).
      addHeader(OidcClaimEmail(AuthorizationService.mock.email)).
      addHeader(OidcClaimSub(AuthorizationService.mock.id)).
      addHeader(OidcClaimUserId(AuthorizationService.mock.id)) ~>
    webService.authorizationRoute ~> check {
      status shouldEqual StatusCodes.OK
    }
  }
}
