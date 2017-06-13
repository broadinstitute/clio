package org.broadinstitute.clio.webservice

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import org.broadinstitute.clio.service.{AuthorizationInfo, AuthorizationService}
import akka.http.scaladsl.model.headers.{ModeledCustomHeader, ModeledCustomHeaderCompanion}
import scala.util.Try


trait OidcHeader {
  def renderInRequests():Boolean = true
  def renderInResponses():Boolean = true
}

class OidcAccessToken(s: String) extends ModeledCustomHeader[OidcAccessToken] with OidcHeader {
  override val value: String = s
  override val companion = OidcAccessToken
}

object OidcAccessToken extends ModeledCustomHeaderCompanion[OidcAccessToken] {
  override val name = "OIDC_access_token"
  override def parse(s: String) = Try(new OidcAccessToken(s))
}


class OidcClaimExpiresIn(s: String) extends ModeledCustomHeader[OidcClaimExpiresIn] with OidcHeader {
  override val value: String = s
  override val companion = OidcClaimExpiresIn
}

object OidcClaimExpiresIn extends ModeledCustomHeaderCompanion[OidcClaimExpiresIn] {
  override val name = "OIDC_CLAIM_expires_in"
  override def parse(s: String) = Try(new OidcClaimExpiresIn(s))
}


class OidcClaimEmail(s: String) extends ModeledCustomHeader[OidcClaimEmail] with OidcHeader {
  override val value: String = s
  override val companion = OidcClaimEmail
}

object OidcClaimEmail extends ModeledCustomHeaderCompanion[OidcClaimEmail] {
  override val name = "OIDC_CLAIM_email"
  override def parse(s: String) = Try(new OidcClaimEmail(s))
}


class OidcClaimSub(s: String) extends ModeledCustomHeader[OidcClaimSub] with OidcHeader {
  override val value: String = s
  override val companion = OidcClaimSub
}

object OidcClaimSub extends ModeledCustomHeaderCompanion[OidcClaimSub] {
  override val name = "OIDC_CLAIM_sub"
  override def parse(s: String) = Try(new OidcClaimSub(s))
}


class OidcClaimUserId(s: String) extends ModeledCustomHeader[OidcClaimUserId] with OidcHeader {
  override val value: String = s
  override val companion = OidcClaimUserId
}

object OidcClaimUserId extends ModeledCustomHeaderCompanion[OidcClaimUserId] {
  override val name = "OIDC_CLAIM_user_id"
  override def parse(s: String) = Try(new OidcClaimUserId(s))
}


trait AuthorizationWebService {

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
