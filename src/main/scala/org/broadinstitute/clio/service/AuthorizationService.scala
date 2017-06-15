package org.broadinstitute.clio.service

import scala.concurrent.{ExecutionContext, Future}

/**
  * A value used to authorize user access to Elasticsearch.
  *
  * @param token   is an access token
  * @param expires is the seconds until `token` expires
  * @param email   is the address for the user
  * @param id      is a Google subject ID or Oauth user ID.
  */
case class AuthorizationInfo(token: String, expires: Long, email: String, id: String)

class AuthorizationService()(implicit ec: ExecutionContext) {

  /**
    * Return `true` when credentials in `info` authorize access to
    * Elasticsearch.  Otherwise return `false`.
    *
    * @param info is the authorization credentials
    * @return is `true` when user is authorized, `false` otherwise
    */
  def authorize(info: AuthorizationInfo): Future[Boolean] =
    Future(AuthorizationService.mock == info)
}

object AuthorizationService {
  def apply()(implicit ec: ExecutionContext): AuthorizationService = {
    new AuthorizationService()
  }

  // Remove this when removing the `/authorization` endpoint.
  //
  /**
    * Mock credentials for testing with the `/authorization` endpoint.
    */
  val mock = AuthorizationInfo(
    "Oidc_access_token",
    1234567890,
    "OIDC_CLAIM_email",
    "OIDC_CLAIM_sub or OIDC_CLAIM_user_id")
}
