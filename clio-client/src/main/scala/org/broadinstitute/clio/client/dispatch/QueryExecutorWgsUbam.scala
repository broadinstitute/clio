package org.broadinstitute.clio.client.dispatch

import org.broadinstitute.clio.client.commands.QueryWgsUbam
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient

import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import io.circe.Json

import scala.concurrent.{ExecutionContext, Future}

class QueryExecutorWgsUbam(queryWgsUbam: QueryWgsUbam) extends Executor {
  override def execute(webClient: ClioWebClient, ioUtil: IoUtil)(
    implicit ec: ExecutionContext,
    bearerToken: OAuth2BearerToken
  ): Future[HttpResponse] = {
    for {
      response <- webClient.queryWgsUbam(
        input = queryWgsUbam.transferWgsUbamV1QueryInput
      )
      resultsAsJson <- webClient.unmarshal[Json](response)
    } yield {
      println(resultsAsJson)
      response
    }
  }
}
