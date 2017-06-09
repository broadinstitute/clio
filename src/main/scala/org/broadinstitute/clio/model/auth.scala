package org.broadinstitute.clio.model

import akka.http.scaladsl.model.headers.{ModeledCustomHeader, ModeledCustomHeaderCompanion}

import scala.util.Try


trait OidcHeader {
  def renderInRequests():Boolean = true
  def renderInResponses():Boolean = true
}

class OidcAccessToken(s: String) extends ModeledCustomHeader[OidcAccessToken] with OidcHeader {
  override def value: String = s
  override val companion = OidcAccessToken
}

object OidcAccessToken extends ModeledCustomHeaderCompanion[OidcAccessToken] {
  override val name = "OIDC_access_token"
  override def parse(s: String) = Try(new OidcAccessToken(s))
}


class OidcClaimExpiresIn(s: String) extends ModeledCustomHeader[OidcClaimExpiresIn] with OidcHeader {
  override def value: String = s
  override val companion = OidcClaimExpiresIn
}

object OidcClaimExpiresIn extends ModeledCustomHeaderCompanion[OidcClaimExpiresIn] {
  override val name = "OIDC_CLAIM_expires_in"
  override def parse(s: String) = Try(new OidcClaimExpiresIn(s))
}


class OidcClaimEmail(s: String) extends ModeledCustomHeader[OidcClaimEmail] with OidcHeader {
  override def value: String = s
  override val companion = OidcClaimEmail
}

object OidcClaimEmail extends ModeledCustomHeaderCompanion[OidcClaimEmail] {
  override val name = "OIDC_CLAIM_email"
  override def parse(s: String) = Try(new OidcClaimEmail(s))
}


class OidcClaimSub(s: String) extends ModeledCustomHeader[OidcClaimSub] with OidcHeader {
  override def value: String = s
  override val companion = OidcClaimSub
}

object OidcClaimSub extends ModeledCustomHeaderCompanion[OidcClaimSub] {
  override val name = "OIDC_CLAIM_sub"
  override def parse(s: String) = Try(new OidcClaimSub(s))
}


class OidcClaimUserId(s: String) extends ModeledCustomHeader[OidcClaimUserId] with OidcHeader {
  override def value: String = s
  override val companion = OidcClaimUserId
}

object OidcClaimUserId extends ModeledCustomHeaderCompanion[OidcClaimUserId] {
  override val name = "OIDC_CLAIM_user_id"
  override def parse(s: String) = Try(new OidcClaimUserId(s))
}
