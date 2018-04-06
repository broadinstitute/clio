package org.broadinstitute.clio.server.webservice

import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import io.circe.syntax._
import org.broadinstitute.clio.server.exceptions.UpsertValidationException
import org.broadinstitute.clio.server.model.ErrorResult

class ExceptionDirectives extends JsonWebService {

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
      case upsertException: UpsertValidationException =>
        complete(
          StatusCodes.BadRequest -> upsertException.message
        )
      case _: Exception => completeWithInternalErrorJsonRoute
    }
  }

  val completeWithInternalErrorJson: Directive0 = {
    handleExceptions(completeWithInternalErrorJsonHandler)
  }
}
