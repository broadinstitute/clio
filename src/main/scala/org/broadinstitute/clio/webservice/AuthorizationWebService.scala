package org.broadinstitute.clio.webservice

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import org.broadinstitute.clio.service.{AuthorizationInfo, AuthorizationService}


trait AuthorizationWebService {

  def authorizationService: AuthorizationService

  val authorize1: Directive1[Option[AuthorizationInfo]] = {
    for {
      token   <- optionalHeaderValueByName("OIDC_access_token")
      expires <- optionalHeaderValueByName("OIDC_CLAIM_expires_in")
      email   <- optionalHeaderValueByName("OIDC_CLAIM_email")
      subject <- optionalHeaderValueByName("OIDC_CLAIM_sub")
      user    <- optionalHeaderValueByName("OIDC_CLAIM_user_id")
    } yield for {
      token   <- token
      expires <- expires
      email   <- email
      id      <- subject orElse user
    } yield AuthorizationInfo(token, expires, email, id)
  }

  val authorize0: Directive0 = authorize1 flatMap {
    _ match {
      case Some(info) => authorizeAsync(authorizationService.authorize(info))
      case None => authorize(false)
    }
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
