package org.broadinstitute.clio.client.dispatch

import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.headers.HttpCredentials
import io.circe.Json
import org.broadinstitute.clio.client.commands._
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient

import scala.concurrent.{ExecutionContext, Future}

/**
  * Executor for all commands that retrieve some data from the
  * clio-server and then print it as JSON.
  */
class RetrieveAndPrintExecutor(command: ClioCommand) extends Executor {
  override def execute(webClient: ClioWebClient, ioUtil: IoUtil)(
    implicit ec: ExecutionContext,
    credentials: HttpCredentials
  ): Future[HttpResponse] = {
    val responseFut = command match {
      case GetServerHealth              => webClient.getClioServerHealth
      case GetServerVersion             => webClient.getClioServerVersion
      case command: GetSchemaCommand[_] => webClient.getSchema(command.index)
      case command: QueryCommand[_] => {
        import command.index.implicits._

        webClient.query(
          command.index,
          command.queryInput,
          command.includeDeleted
        )
      }
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
