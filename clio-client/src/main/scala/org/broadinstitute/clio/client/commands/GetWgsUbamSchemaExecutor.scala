package org.broadinstitute.clio.client.commands

import org.broadinstitute.clio.client.dispatch.Executor
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient

import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import io.circe.Json

import scala.concurrent.{ExecutionContext, Future}

object GetWgsUbamSchemaExecutor extends Executor {
  override def execute(webClient: ClioWebClient, ioUtil: IoUtil)(
    implicit ec: ExecutionContext,
    bearerToken: OAuth2BearerToken
  ): Future[HttpResponse] = {
    for {
      response <- webClient.getWgsUbamSchema
      schemaAsJson <- webClient.unmarshal[Json](response)
    } yield {
      println(schemaAsJson)
      response
    }
  }
}
