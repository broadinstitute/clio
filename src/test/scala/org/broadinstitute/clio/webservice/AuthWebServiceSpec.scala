package org.broadinstitute.clio.webservice

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.http.scaladsl.server.AuthorizationFailedRejection
// import akka.http.scaladsl.model.StatusCodes
// import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
// import io.circe.generic.auto._
import org.scalatest.{FlatSpec, Matchers}
import org.broadinstitute.clio.model._
import org.broadinstitute.clio.service._

class AuthWebServiceSpec extends FlatSpec
    with Matchers with AuthWebService with ScalatestRouteTest {

  behavior of "AuthWebService"

  it should "authorizationRoute" in {

    Get("/authorization").
      addHeader(OidcClaimExpiresIn(AuthService.mock.expiration)).
      addHeader(OidcClaimEmail(AuthService.mock.email)).
      addHeader(OidcClaimSub(AuthService.mock.id)) ~>
    authorizationRoute ~> check {
      rejection shouldEqual AuthorizationFailedRejection
    }

    Get("/authorization").
      addHeader(OidcAccessToken(AuthService.mock.token)).
      addHeader(OidcClaimEmail(AuthService.mock.email)).
      addHeader(OidcClaimSub(AuthService.mock.id)) ~>
    authorizationRoute ~> check {
      rejection shouldEqual AuthorizationFailedRejection
    }

    Get("/authorization").
      addHeader(OidcAccessToken(AuthService.mock.token)).
      addHeader(OidcClaimExpiresIn(AuthService.mock.expiration)).
      addHeader(OidcClaimSub(AuthService.mock.id)) ~>
    authorizationRoute ~> check {
      rejection shouldEqual AuthorizationFailedRejection
    }

    Get("/authorization").
      addHeader(OidcAccessToken(AuthService.mock.token)).
      addHeader(OidcClaimExpiresIn(AuthService.mock.expiration)).
      addHeader(OidcClaimEmail(AuthService.mock.email)) ~>
    authorizationRoute ~> check {
      rejection shouldEqual AuthorizationFailedRejection
    }

    Get("/authorization").
      addHeader(OidcAccessToken(AuthService.mock.token)).
      addHeader(OidcClaimExpiresIn(AuthService.mock.expiration)).
      addHeader(OidcClaimEmail(AuthService.mock.email)).
      addHeader(OidcClaimSub(AuthService.mock.id)) ~>
    authorizationRoute ~> check {
      status shouldEqual StatusCodes.OK
    }

    Get("/authorization").
      addHeader(OidcAccessToken(AuthService.mock.token)).
      addHeader(OidcClaimExpiresIn(AuthService.mock.expiration)).
      addHeader(OidcClaimEmail(AuthService.mock.email)).
      addHeader(OidcClaimUserId(AuthService.mock.id)) ~>
    authorizationRoute ~> check {
      status shouldEqual StatusCodes.OK
    }
  }
}
