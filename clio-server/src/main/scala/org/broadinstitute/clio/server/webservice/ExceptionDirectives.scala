package org.broadinstitute.clio.webservice

import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import io.circe.generic.auto._
import io.circe.syntax._
import org.broadinstitute.clio.model.ErrorResult

trait ExceptionDirectives {
  lazy val completeWithInternalErrorJson: Directive0 = {
    handleExceptions(completeWithInternalErrorJsonHandler)
  }

  private val completeWithInternalErrorJsonRoute: Route = {
    val message = ErrorResult(StatusCodes.InternalServerError.defaultMessage)
    val jsonMessage = message.asJson.noSpaces
    val entity = HttpEntity(ContentTypes.`application/json`, jsonMessage)
    val httpResponse =
      HttpResponse(StatusCodes.InternalServerError, entity = entity)
    complete(httpResponse)
  }

  private val completeWithInternalErrorJsonHandler: ExceptionHandler = {
    ExceptionHandler {
      case _: Exception => completeWithInternalErrorJsonRoute
    }
  }
}
