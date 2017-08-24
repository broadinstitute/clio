package org.broadinstitute.clio.client.commands

import akka.http.scaladsl.model.HttpResponse
import org.broadinstitute.clio.client.webclient.ClioWebClient

import scala.concurrent.{ExecutionContext, Future}

trait Executor {
  def execute(webClient: ClioWebClient, bearerToken: String)(
    implicit ec: ExecutionContext
  ): Future[HttpResponse]
}
