package org.broadinstitute.clio.client.commands

import akka.http.scaladsl.model.HttpResponse
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.clio.client.ClioClientConfig
import org.broadinstitute.clio.client.webclient.ClioWebClient

import scala.concurrent.{Await, ExecutionContext, Future}

object CommandDispatch extends LazyLogging {

  def dispatch(webClient: ClioWebClient, command: Command, bearerToken: String)(
    implicit ec: ExecutionContext
  ): Boolean = {
    checkResponse(command.execute(webClient, bearerToken = bearerToken))
  }

  def checkResponse(
    responseFuture: Future[HttpResponse]
  )(implicit ec: ExecutionContext): Boolean = {
    Await.result(
      responseFuture.map[Boolean] { response =>
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
      },
      ClioClientConfig.responseTimeout
    )
  }
}
