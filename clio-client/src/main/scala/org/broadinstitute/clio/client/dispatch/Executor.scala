package org.broadinstitute.clio.client.dispatch

import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.headers.HttpCredentials
import com.typesafe.scalalogging.LazyLogging
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport
import org.broadinstitute.clio.client.util.{FutureWithErrorMessage, IoUtil}
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.util.json.ModelAutoDerivation

import scala.concurrent.{ExecutionContext, Future}

trait Executor
    extends LazyLogging
    with FailFastCirceSupport
    with ModelAutoDerivation
    with FutureWithErrorMessage {
  def execute(webClient: ClioWebClient, ioUtil: IoUtil)(
    implicit ec: ExecutionContext,
    credentials: HttpCredentials
  ): Future[HttpResponse]
}
