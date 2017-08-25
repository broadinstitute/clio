package org.broadinstitute.clio.client.commands

import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.clio.client.webclient.ClioWebClient

import scala.concurrent.{ExecutionContext, Future}

class QueryWgsUbamExecutor(queryWgsUbam: QueryWgsUbam) extends Executor {
  override def execute(webClient: ClioWebClient)(
    implicit ec: ExecutionContext,
    bearerToken: OAuth2BearerToken
  ): Future[HttpResponse] = {
    webClient.queryWgsUbam(input = queryWgsUbam.transferWgsUbamV1QueryInput)
  }
}
