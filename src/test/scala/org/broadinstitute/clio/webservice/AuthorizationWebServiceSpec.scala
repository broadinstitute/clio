package org.broadinstitute.clio.webservice

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.http.scaladsl.server.AuthorizationFailedRejection
import org.scalatest.{FlatSpec, Matchers}
import org.broadinstitute.clio.model._
import org.broadinstitute.clio.service._

class AuthWebServiceSpec extends FlatSpec
    with Matchers with AuthorizationWebService with ScalatestRouteTest {

  behavior of "AuthWebService"

  it should "authorizationRoute" in {

    Get("/authorization").
      addHeader(OidcClaimExpiresIn(AuthorizationService.mock.expiration)).
      addHeader(OidcClaimEmail(AuthorizationService.mock.email)).
      addHeader(OidcClaimSub(AuthorizationService.mock.id)) ~>
    authorizationRoute ~> check {
      rejection shouldEqual AuthorizationFailedRejection
    }

    Get("/authorization").
      addHeader(OidcAccessToken(AuthorizationService.mock.token)).
      addHeader(OidcClaimEmail(AuthorizationService.mock.email)).
      addHeader(OidcClaimSub(AuthorizationService.mock.id)) ~>
    authorizationRoute ~> check {
      rejection shouldEqual AuthorizationFailedRejection
    }

    Get("/authorization").
      addHeader(OidcAccessToken(AuthorizationService.mock.token)).
      addHeader(OidcClaimExpiresIn(AuthorizationService.mock.expiration)).
      addHeader(OidcClaimSub(AuthorizationService.mock.id)) ~>
    authorizationRoute ~> check {
      rejection shouldEqual AuthorizationFailedRejection
    }

    Get("/authorization").
      addHeader(OidcAccessToken(AuthorizationService.mock.token)).
      addHeader(OidcClaimExpiresIn(AuthorizationService.mock.expiration)).
      addHeader(OidcClaimEmail(AuthorizationService.mock.email)) ~>
    authorizationRoute ~> check {
      rejection shouldEqual AuthorizationFailedRejection
    }

    Get("/authorization").
      addHeader(OidcAccessToken(AuthorizationService.mock.token)).
      addHeader(OidcClaimExpiresIn(AuthorizationService.mock.expiration)).
      addHeader(OidcClaimEmail(AuthorizationService.mock.email)).
      addHeader(OidcClaimSub(AuthorizationService.mock.id)) ~>
    authorizationRoute ~> check {
      status shouldEqual StatusCodes.OK
    }

    Get("/authorization").
      addHeader(OidcAccessToken(AuthorizationService.mock.token)).
      addHeader(OidcClaimExpiresIn(AuthorizationService.mock.expiration)).
      addHeader(OidcClaimEmail(AuthorizationService.mock.email)).
      addHeader(OidcClaimUserId(AuthorizationService.mock.id)) ~>
    authorizationRoute ~> check {
      status shouldEqual StatusCodes.OK
    }
  }
}
