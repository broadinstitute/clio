package org.broadinstitute.clio.client.dispatch

import com.google.auth.oauth2.OAuth2Credentials
import io.circe.Json
import org.broadinstitute.clio.client.commands._
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient

import scala.concurrent.{ExecutionContext, Future}

/**
  * Executor for all commands that retrieve some data from the
  * clio-server and then print it as JSON.
  */
class RetrieveAndPrintExecutor(command: RetrieveAndPrintCommand)(
  implicit credentials: OAuth2Credentials
) extends Executor[Json] {
  override def execute(webClient: ClioWebClient, ioUtil: IoUtil)(
    implicit ec: ExecutionContext
  ): Future[Json] = {
    val responseFut = command match {
      case GetServerHealth  => webClient.getClioServerHealth
      case GetServerVersion => webClient.getClioServerVersion
      case getSchema: GetSchemaCommand[_] =>
        webClient.getSchema(getSchema.index)
      case query: QueryCommand[_] => {
        webClient.query(query.index)(query.queryInput, query.includeDeleted)
      }
    }

    for {
      resultsAsJson <- responseFut
    } yield {
      // Pretty-print the JSON, using 2 spaces for indentation.
      println(resultsAsJson.spaces2)
      resultsAsJson
    }
  }
}
