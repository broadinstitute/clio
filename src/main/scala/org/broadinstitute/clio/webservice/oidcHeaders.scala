package org.broadinstitute.clio.webservice

import akka.http.scaladsl.model.headers.{ModeledCustomHeader, ModeledCustomHeaderCompanion}

import scala.util.Try


/**
  * An OIDC access token with value {@param token}.
  * @param token is an access token
  */
class OidcAccessToken(val token: String) extends ModeledCustomHeader[OidcAccessToken] with RequestResponseHeader {
  override val value: String = token
  override val companion = OidcAccessToken
}

object OidcAccessToken extends ModeledCustomHeaderCompanion[OidcAccessToken] {
  override val name = "OIDC_access_token"
  override def parse(token: String) = Try(new OidcAccessToken(token))
}

/**
  * An OIDC token expiration claim with value {@param seconds}.
  * @param seconds integer seconds to token expiration
  */
class OidcClaimExpiresIn(val seconds: Long) extends ModeledCustomHeader[OidcClaimExpiresIn] with RequestResponseHeader {
  override val value: String = seconds.toString
  override val companion = OidcClaimExpiresIn
}

object OidcClaimExpiresIn extends ModeledCustomHeaderCompanion[OidcClaimExpiresIn] {
  override val name = "OIDC_CLAIM_expires_in"
  override def parse(seconds: String) = Try(new OidcClaimExpiresIn(seconds.toLong))
  def apply(seconds: Long) = new OidcClaimExpiresIn(seconds)
}

/**
  * An OIDC email claim with address {@param email}.
  * @param email is an email address
  */
class OidcClaimEmail(val email: String) extends ModeledCustomHeader[OidcClaimEmail] with RequestResponseHeader {
  override val value: String = email
  override val companion = OidcClaimEmail
}

object OidcClaimEmail extends ModeledCustomHeaderCompanion[OidcClaimEmail] {
  override val name = "OIDC_CLAIM_email"
  override def parse(s: String) = Try(new OidcClaimEmail(s))
}

/**
  * An OIDC subject ID claim with value {@param subject}.
  * @param subject is a Google subject ID
  */
class OidcClaimSub(val subject: String) extends ModeledCustomHeader[OidcClaimSub] with RequestResponseHeader {
  override val value: String = subject
  override val companion = OidcClaimSub
}

object OidcClaimSub extends ModeledCustomHeaderCompanion[OidcClaimSub] {
  override val name = "OIDC_CLAIM_sub"
  override def parse(subject: String) = Try(new OidcClaimSub(subject))
}

/**
  * An OIDC user ID claim with value {@param user}.
  * @param user is a user ID claim
  */
class OidcClaimUserId(val user: String) extends ModeledCustomHeader[OidcClaimUserId] with RequestResponseHeader {
  override val value: String = user
  override val companion = OidcClaimUserId
}

object OidcClaimUserId extends ModeledCustomHeaderCompanion[OidcClaimUserId] {
  override val name = "OIDC_CLAIM_user_id"
  override def parse(user: String) = Try(new OidcClaimUserId(user))
}
