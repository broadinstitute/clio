package org.broadinstitute.clio.client.commands

import akka.http.scaladsl.model.HttpResponse
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.clio.client.ClioClientConfig

import scala.concurrent.{Await, ExecutionContext, Future}

object Commands {
  val addWgsUbam = "addWgsUbam"
  val queryWgsUbam = "queryWgsUbam"
}

abstract class Command(name: String)(implicit ec: ExecutionContext)
    extends LazyLogging {

  def execute: Boolean

  def checkResponse(responseFuture: Future[HttpResponse]): Boolean = {
    Await.result(
      responseFuture.map[Boolean] { response =>
        val isSuccess = response.status.isSuccess()

        if (isSuccess) {
          logger.info(
            s"Successfully completed command $name." +
              s" Response code: ${response.status}"
          )
        } else {
          logger.error(
            s"Error executing command $name." +
              s" Response code: ${response.status}"
          )
        }
        isSuccess
      },
      ClioClientConfig.responseTimeout
    )
  }
}
