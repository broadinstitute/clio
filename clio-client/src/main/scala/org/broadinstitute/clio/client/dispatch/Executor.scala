package org.broadinstitute.clio.client.dispatch

import com.google.auth.oauth2.OAuth2Credentials
import com.typesafe.scalalogging.LazyLogging
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport
import org.broadinstitute.clio.client.util.{FutureWithErrorMessage, IoUtil}
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.util.json.ModelAutoDerivation

import scala.concurrent.{ExecutionContext, Future}

abstract class Executor[Out](implicit val credentials: OAuth2Credentials)
    extends LazyLogging
    with FailFastCirceSupport
    with ModelAutoDerivation
    with FutureWithErrorMessage {

  def execute(webClient: ClioWebClient, ioUtil: IoUtil)(
    implicit ec: ExecutionContext
  ): Future[Out]
}
