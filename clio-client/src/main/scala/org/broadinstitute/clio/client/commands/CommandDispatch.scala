package org.broadinstitute.clio.client.commands

import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.clio.client.ClioClientConfig
import org.broadinstitute.clio.client.webclient.ClioWebClient

import scala.concurrent.{Await, ExecutionContext, Future}

object CommandDispatch extends LazyLogging {

  def execute(command: CommandType, webClient: ClioWebClient)(
    implicit ec: ExecutionContext,
    bearerToken: OAuth2BearerToken
  ): Future[HttpResponse] = {
    command match {
      case add: AddWgsUbam =>
        new AddWgsUbamExecutor(add)
          .execute(webClient)
      case query: QueryWgsUbam =>
        new QueryWgsUbamExecutor(query).execute(webClient)
    }
  }

  def dispatch(
    webClient: ClioWebClient,
    command: CommandType
  )(implicit ec: ExecutionContext, bearerToken: OAuth2BearerToken): Boolean = {
    checkResponse(execute(command, webClient))
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
