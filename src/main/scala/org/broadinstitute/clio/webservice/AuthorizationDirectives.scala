package org.broadinstitute.clio.webservice

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import org.broadinstitute.clio.service.{AuthorizationInfo, AuthorizationService}


trait AuthorizationDirectives {

  def authorizationService: AuthorizationService

  val authorize1: Directive1[Option[AuthorizationInfo]] = extractRequest flatMap {
    request => {
      val args = request.headers.collect {
        case OidcAccessToken(v)    => 'token   -> v
        case OidcClaimExpiresIn(v) => 'expires -> v
        case OidcClaimEmail(v)     => 'email   -> v
        case OidcClaimSub(v)       => 'subject -> v
        case OidcClaimUserId(v)    => 'user    -> v
      }.toMap
      val id = (args get 'subject) orElse (args get 'user)
      val info = (args get 'token, args get 'expires, args get 'email, id) match {
        case (Some(t), Some(x), Some(m), Some(i)) => Some(AuthorizationInfo(t, x, m, i))
        case _ => None
      }
      provide(info)
    }
  }

  val authorize0: Directive0 = authorize1 flatMap {
    case Some(info) => authorizeAsync(authorizationService.authorize(info))
    case None => authorize(false)
  }

  val authorizationRoute: Route =
    path("authorization") {
      authorize0 {
        get {
          complete("OK")
        }
      }
    }
}
