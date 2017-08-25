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
  )(implicit ec: ExecutionContext): Future[Boolean] = {
    config.command
      .map(command => execute(command, config))
      .fold(Future(false))(checkResponse)
  }

  def checkResponse(
    responseFuture: Future[HttpResponse]
  )(implicit ec: ExecutionContext): Future[Boolean] = {
    responseFuture.map { response =>
      val isSuccess = response.status.isSuccess()
      if (isSuccess) {
        logger.info(
          s"Successfully completed command." +
            s" Response code: ${response.status}"
        )
      } else {
        logger.error(
          s"Error executing command." +
            s" Response code: ${response.status}"
        )
      }
      logger.info(response.toString)
      isSuccess
    }
  }
}
