package org.broadinstitute.clio.client.commands

import akka.http.scaladsl.model.HttpResponse
import org.broadinstitute.clio.client.webclient.ClioWebClient

import scala.concurrent.{ExecutionContext, Future}


class QueryWgsUbamExecutor(queryWgsUbam: QueryWgsUbam) extends Executor {
  override def execute(webClient: ClioWebClient, bearerToken: String)(
    implicit ec: ExecutionContext
  ): Future[HttpResponse] = {
    webClient.queryWgsUbam(
      bearerToken = bearerToken,
      input = queryWgsUbam.transferWgsUbamV1QueryInput
    )
  }
}
