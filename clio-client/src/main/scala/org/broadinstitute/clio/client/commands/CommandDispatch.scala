package org.broadinstitute.clio.client.commands

import akka.http.scaladsl.model.HttpResponse
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.clio.client.commands.Commands.{
  AddWgsUbam,
  MoveWgsUbam,
  QueryWgsUbam
}
import org.broadinstitute.clio.client.parser.BaseArgs
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class CommandDispatch(val webClient: ClioWebClient, val ioUtil: IoUtil)
    extends LazyLogging {

  private def execute(command: CommandType, config: BaseArgs)(
    implicit ec: ExecutionContext
  ): Future[HttpResponse] = {
    command match {
      case AddWgsUbam => AddWgsUbamCommand.execute(webClient, config, ioUtil)
      case QueryWgsUbam =>
        QueryWgsUbamCommand.execute(webClient, config, ioUtil)
      case MoveWgsUbam => MoveWgsUbamCommand.execute(webClient, config, ioUtil)
    }
  }

  def dispatch(
    config: BaseArgs
  )(implicit ec: ExecutionContext): Future[HttpResponse] = {
    config.command
      .map(command => execute(command, config))
      .fold(
        Future
          .failed[HttpResponse](new Exception("The config command was empty"))
      )(checkResponse)
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
