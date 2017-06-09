package org.broadinstitute.clio.service

import akka.http.scaladsl.server.RequestContext
import org.broadinstitute.clio.model._


case class AuthorizationInfo(token: String, expiration: String, email: String, id: String)

class AuthorizationService() {

  def authorize(context: RequestContext): Boolean = {
    val pairs = for (header <- context.request.headers) yield
      header match {
        case OidcAccessToken(token)      => 0 -> token
        case OidcClaimExpiresIn(expires) => 1 -> expires
        case OidcClaimEmail(email)       => 2 -> email
        case OidcClaimUserId(id)         => 3 -> id
        case OidcClaimSub(id)            => 3 -> id
      }
    (pairs.size == 4) && (Vector.tabulate(4) { pairs.toMap } match {
      case Vector(token, expires, email, id) =>
        AuthorizationService.mock == AuthorizationInfo(token, expires, email, id)
      case _ => false
    })
  }
}

object AuthorizationService {
  def apply(): AuthorizationService = {
    new AuthorizationService()
  }

  val mock = AuthorizationInfo("token", "expiration", "email", "id")
}
