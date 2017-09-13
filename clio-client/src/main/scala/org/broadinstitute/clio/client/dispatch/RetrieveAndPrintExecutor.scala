package org.broadinstitute.clio.client.dispatch
import org.broadinstitute.clio.client.commands._
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient

import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import io.circe.Json

import scala.concurrent.{ExecutionContext, Future}

/**
  * Executor for all commands that retrieve some data from the
  * clio-server and then print it as JSON.
  */
class RetrieveAndPrintExecutor(command: ClioCommand) extends Executor {
  override def execute(webClient: ClioWebClient, ioUtil: IoUtil)(
    implicit ec: ExecutionContext,
    bearerToken: OAuth2BearerToken
  ): Future[HttpResponse] = {
    val responseFut = command match {
      case GetServerHealth  => webClient.getClioServerHealth
      case GetServerVersion => webClient.getClioServerVersion
      case GetSchemaWgsUbam => webClient.getSchemaWgsUbam
      case GetSchemaGvcf    => webClient.getSchemaGvcf
      case queryWgsUbam: QueryWgsUbam =>
        webClient.queryWgsUbam(
          queryWgsUbam.transferWgsUbamV1QueryInput,
          queryWgsUbam.includeDeleted
        )
      case queryGvcf: QueryGvcf =>
        webClient.queryGvcf(
          queryGvcf.transferGvcfV1QueryInput,
          queryGvcf.includeDeleted
        )
      case other =>
        throw new RuntimeException(
          s"${getClass.getName} cannot handle command of type $other"
        )
    }

    for {
      response <- responseFut
      resultsAsJson <- webClient.unmarshal[Json](response)
    } yield {
      println(resultsAsJson)
      response
    }
  }
}