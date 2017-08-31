package org.broadinstitute.clio.client.commands

import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.clio.client.commands.Commands.{AddWgsUbam, CommandType, DeleteWgsUbam, MoveWgsUbam, QueryWgsUbam}
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class CommandDispatch(val webClient: ClioWebClient, val ioUtil: IoUtil)
    extends LazyLogging {

  def execute(command: CommandType, webClient: ClioWebClient)(
    implicit ec: ExecutionContext,
    bearerToken: OAuth2BearerToken
  ): Future[HttpResponse] = {
    command match {
      case add: AddWgsUbam =>
        new AddWgsUbamExecutor(add).execute(webClient, ioUtil)
      case query: QueryWgsUbam =>
        new QueryWgsUbamExecutor(query).execute(webClient, ioUtil)
      case move: MoveWgsUbam =>
        new MoveWgsUbamExecutor(move).execute(webClient, ioUtil)
      case delete: DeleteWgsUbam =>
        new DeleteWgsUbamExecutor(delete).execute(webClient,ioUtil)
    }
  }

  def dispatch(command: CommandType)(
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
          logger.error(
            s"Error executing command." +
              s" Response code: ${response.status}"
          )
          Future.failed(new Exception("The call to Clio was unsuccessful"))
        }
      case Failure(ex) => Future.failed(ex)
    }
  }
}
