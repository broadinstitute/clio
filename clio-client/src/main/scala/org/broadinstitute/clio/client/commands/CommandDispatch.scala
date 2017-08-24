package org.broadinstitute.clio.client.commands

import akka.http.scaladsl.model.HttpResponse
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.clio.client.commands.Commands.{
  AddWgsUbam,
  MoveWgsUbam,
  QueryWgsUbam
}
import org.broadinstitute.clio.client.parser.BaseArgs
import org.broadinstitute.clio.client.webclient.ClioWebClient

import scala.concurrent.{ExecutionContext, Future}

object CommandDispatch extends LazyLogging {

  def execute(command: CommandType, webClient: ClioWebClient, config: BaseArgs)(
    implicit ec: ExecutionContext
  ): Future[HttpResponse] = {
    command match {
      case AddWgsUbam   => AddWgsUbamCommand.execute(webClient, config)
      case QueryWgsUbam => QueryWgsUbamCommand.execute(webClient, config)
      case MoveWgsUbam  => MoveWgsUbamCommand.execute(webClient, config)
    }
  }

  def dispatch(webClient: ClioWebClient, config: BaseArgs)(
    implicit ec: ExecutionContext
  ): Future[Boolean] = {
    config.command
      .map(command => execute(command, webClient, config))
      .fold(Future(false))(checkResponse)
  }

  def checkResponse(
    responseFuture: Future[HttpResponse]
  )(implicit ec: ExecutionContext): Future[Boolean] = {
    responseFuture.map(response => {
      val isSuccess = response.status.isSuccess()
      if (isSuccess) {
        logger.info(
          s"Successfully completed command." +
            s" Response code: ${response.status}"
        )
        logger.info(response.toString)
      } else {
        logger.error(
          s"Error executing command." +
            s" Response code: ${response.status}"
        )
        logger.info(response.toString)
      }
      isSuccess
    })
  }
}
