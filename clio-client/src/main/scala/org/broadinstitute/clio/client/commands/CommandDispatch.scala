package org.broadinstitute.clio.client.commands

import akka.http.scaladsl.model.HttpResponse
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.clio.client.ClioClientConfig
import org.broadinstitute.clio.client.webclient.ClioWebClient

import scala.concurrent.{Await, ExecutionContext, Future}

object CommandDispatch extends LazyLogging {

  def execute(
    command: CommandType,
    webClient: ClioWebClient,
    bearerToken: String
  )(implicit ec: ExecutionContext): Future[HttpResponse] = {
    command match {
      case commandType: AddWgsUbam =>
        new AddWgsUbamExecutor(commandType)
          .execute(webClient, bearerToken)
      case commandType: QueryWgsUbam =>
        new QueryWgsUbamExecutor(commandType).execute(webClient, bearerToken)
    }
  }

  def dispatch(webClient: ClioWebClient,
               command: CommandType,
               bearerToken: String)(implicit ec: ExecutionContext): Boolean = {
    checkResponse(execute(command, webClient, bearerToken = bearerToken))
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
