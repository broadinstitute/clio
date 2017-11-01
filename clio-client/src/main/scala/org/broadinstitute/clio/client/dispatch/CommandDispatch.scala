package org.broadinstitute.clio.client.dispatch

import org.broadinstitute.clio.client.commands._
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient

import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class CommandDispatch(val webClient: ClioWebClient, val ioUtil: IoUtil)
    extends LazyLogging {

  def execute(command: ClioCommand, webClient: ClioWebClient)(
    implicit ec: ExecutionContext,
    bearerToken: OAuth2BearerToken
  ): Future[HttpResponse] = {
    (command match {
      case addCommand: AddCommand[_]       => new AddExecutor(addCommand)
      case moveCommand: MoveCommand[_]     => new MoveExecutor(moveCommand)
      case deleteCommand: DeleteCommand[_] => new DeleteExecutor(deleteCommand)
      case deliverCommand: DeliverWgsCram =>
        new DeliverWgsCramExecutor(deliverCommand)
      case other =>
        new RetrieveAndPrintExecutor(other)
    }).execute(webClient, ioUtil)
  }

  def dispatch(command: ClioCommand)(
    implicit ec: ExecutionContext,
    bearerToken: OAuth2BearerToken
  ): Future[HttpResponse] = {
    checkResponse(execute(command, webClient))
  }

  def checkResponse(
    responseFuture: Future[HttpResponse]
  )(implicit ec: ExecutionContext): Future[HttpResponse] = {
    responseFuture.transformWith {
      case Success(response) =>
        if (response.status.isSuccess()) {
          logger.info(
            s"Successfully completed command." +
              s" Response code: ${response.status}"
          )
          responseFuture
        } else {
          Future.failed(
            new Exception(
              s"The call to Clio was unsuccessful. Response code: ${response.status}"
            )
          )
        }
      case Failure(ex) => Future.failed(ex)
    }
  }
}
