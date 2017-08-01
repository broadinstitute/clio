package org.broadinstitute.clio.client.commands

import akka.http.scaladsl.model.HttpResponse
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.clio.client.ClioClientConfig
import org.broadinstitute.clio.client.webclient.ClioWebClient

import scala.concurrent.{Await, Future}

object Commands {
  val addReadGroupBam = "addReadGroupBam"
  val queryReadGroupBam = "queryReadGroupBam"
}

abstract class Command(clioWebClient: ClioWebClient) extends LazyLogging {

  def execute: Boolean

  def shutdown(): Unit = {
    logger.info("Shutting down clio web client", clioWebClient.shutdown())
  }

  def checkResponseAndShutdown(
    commandName: String,
    responseFuture: Future[HttpResponse]
  ): Boolean = {
    val response = checkResponse(commandName, responseFuture)
    shutdown()
    response.status.isSuccess
  }

  def checkResponse(commandName: String,
                    responseFuture: Future[HttpResponse]): HttpResponse = {
    import scala.concurrent.ExecutionContext.Implicits.global
    Await.result(
      responseFuture.map[HttpResponse] { response =>
        if (response.status.isSuccess()) {
          logger.info(
            s"Successfully completed command $commandName." +
              s" Response code: ${response.status}"
          )
        } else {
          logger.error(
            s"Error executing command $commandName." +
              s" Response code: ${response.status}"
          )
        }
        response
      },
      ClioClientConfig.responseTimeout
    )
  }
}
