package org.broadinstitute.clio.model

import akka.http.scaladsl.model.headers.{ModeledCustomHeader, ModeledCustomHeaderCompanion}

import scala.util.Try


abstract class OidcHeader(s: String) extends ModeledCustomHeader[OidcHeader] {
  override def value = s
  override val renderInRequests = true
  override val renderInResponses = true
}

class OidcAccessToken(s: String) extends ModeledCustomHeader[OidcAccessToken] {
  override def value: String = s
  override def renderInRequests(): Boolean = true
  override def renderInResponses(): Boolean = true
  override val companion = OidcAccessToken
}

object OidcAccessToken extends ModeledCustomHeaderCompanion[OidcAccessToken] {
  override val name = "OIDC_access_token"
  override def parse(s: String) = Try(new OidcAccessToken(s))
}


class OidcClaimExpiresIn(s: String) extends ModeledCustomHeader[OidcClaimExpiresIn] {
  override def value: String = s
  override def renderInRequests(): Boolean = true
  override def renderInResponses(): Boolean = true
  override val companion = OidcClaimExpiresIn
}

object OidcClaimExpiresIn extends ModeledCustomHeaderCompanion[OidcClaimExpiresIn] {
  override val name = "OIDC_CLAIM_expires_in"
  override def parse(s: String) = Try(new OidcClaimExpiresIn(s))
}


class OidcClaimEmail(s: String) extends ModeledCustomHeader[OidcClaimEmail] {
  override def value: String = s
  override def renderInRequests(): Boolean = true
  override def renderInResponses(): Boolean = true
  override val companion = OidcClaimEmail
}

object OidcClaimEmail extends ModeledCustomHeaderCompanion[OidcClaimEmail] {
  override val name = "OIDC_CLAIM_email"
  override def parse(s: String) = Try(new OidcClaimEmail(s))
}


class OidcClaimSub(s: String) extends ModeledCustomHeader[OidcClaimSub] {
  override def value: String = s
  override def renderInRequests(): Boolean = true
  override def renderInResponses(): Boolean = true
  override val companion = OidcClaimSub
}

object OidcClaimSub extends ModeledCustomHeaderCompanion[OidcClaimSub] {
  override val name = "OIDC_CLAIM_sub"
  override def parse(s: String) = Try(new OidcClaimSub(s))
}


class OidcClaimUserId(s: String) extends ModeledCustomHeader[OidcClaimUserId] {
  override def value: String = s
  override def renderInRequests(): Boolean = true
  override def renderInResponses(): Boolean = true
  override val companion = OidcClaimUserId
}

object OidcClaimUserId extends ModeledCustomHeaderCompanion[OidcClaimUserId] {
  override val name = "OIDC_CLAIM_user_id"
  override def parse(s: String) = Try(new OidcClaimUserId(s))
}
