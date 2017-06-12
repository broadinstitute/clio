package org.broadinstitute.clio.service

import scala.concurrent.{ExecutionContext, Future}


case class AuthorizationInfo(token: String, expires: String, email: String, id: String)

class AuthorizationService()(implicit ec: ExecutionContext) {
  def authorize(info: AuthorizationInfo): Future[Boolean] =
    Future(AuthorizationService.mock == info)
}

object AuthorizationService {
  def apply()(implicit ec: ExecutionContext): AuthorizationService = {
    new AuthorizationService()
  }
  val mock = AuthorizationInfo("token", "expires", "email", "id")
}
