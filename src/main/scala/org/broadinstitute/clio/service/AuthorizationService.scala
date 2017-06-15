package org.broadinstitute.clio.service

import scala.concurrent.{ExecutionContext, Future}


case class AuthorizationInfo(token: String, expires: Long, email: String, id: String)

class AuthorizationService()(implicit ec: ExecutionContext) {
  def authorize(info: AuthorizationInfo): Future[Boolean] =
    Future(AuthorizationService.mock == info)
}

object AuthorizationService {
  def apply()(implicit ec: ExecutionContext): AuthorizationService = {
    new AuthorizationService()
  }
  val mock = AuthorizationInfo(
    "OIDC_access_token",
    1234567890,
    "OIDC_CLAIM_email",
    "OIDC_CLAIM_sub or OIDC_CLAIM_user_id")
}
