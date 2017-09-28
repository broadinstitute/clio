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
        /*
         * NOTE: Because of how path-dependent types work in Scala, this *has*
         * to be `command.index.queryInputEncoder` (pulling `command.index`
         * into a different variable will break everything).
         *
         * I think this is because if you say `val index = command.index` and
         * `implicit val encoder = index.queryInputEncoder`, then you'll have an
         * `Encoder[index.QueryInputType]`, but the type of `command.queryInput`
         * is `command.index.QueryInputType`, and the typechecker doesn't know
         * that `index` is the same as `command.index`, so it says the types don't
         * match and everything explodes.
         */
        implicit val encoder = command.index.queryInputEncoder
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
