package org.broadinstitute.clio.webservice

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import org.broadinstitute.clio.service.{
  AuthorizationInfo,
  AuthorizationService
}

trait AuthorizationDirectives {

  def authorizationService: AuthorizationService

  /**
    * Extract an `AuthorizationInfo` from request headers.
    */
  val optionalOidcAuthorizationInfo: Directive1[Option[AuthorizationInfo]] = {
    for {
      token <- optionalHeaderValueByType[OidcAccessToken](())
      expires <- optionalHeaderValueByType[OidcClaimExpiresIn](())
      email <- optionalHeaderValueByType[OidcClaimEmail](())
      subject <- optionalHeaderValueByType[OidcClaimSub](())
      user <- optionalHeaderValueByType[OidcClaimUserId](())
    } yield
      for {
        token <- token.map(_.token)
        expires <- expires.map(_.seconds)
        email <- email.map(_.email)
        id <- subject.map(_.subject) orElse user.map(_.user)
      } yield AuthorizationInfo(token, expires, email, id)
  }

  /**
    * Use OIDC header values to authorize a request.
    */
  val authorizeOidc: Directive0 = optionalOidcAuthorizationInfo flatMap {
    case Some(info) => authorizeAsync(authorizationService.authorize(info))
    case None       => authorize(false)
  }

  /**
    * A temporary endpoint to test `authorizeOidc`.
    */
  val authorizationRoute: Route =
    path("authorization") {
      authorizeOidc {
        get {
          complete("OK")
        }
      }
    }
}
