package org.broadinstitute.clio.server.webservice

import java.time.OffsetDateTime

import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import io.circe.syntax._
import org.broadinstitute.clio.server.model.{
  ApiNotReadyRejection,
  ApiNotReadyResult,
  RejectionResult
}

/** Rejection translation into json. */
class RejectionDirectives(serverStartTime: OffsetDateTime) extends JsonWebService {

  private val mapRejectionsToJsonHandler: RejectionHandler = {
    RejectionHandler
      .newBuilder()
      .handle {
        case ApiNotReadyRejection => {
          complete(
            StatusCodes.ServiceUnavailable -> ApiNotReadyResult(
              "Clio is rebuilding search index, try again later",
              serverStartTime
            )
          )
        }
      }
      .result()
      .seal
      .mapRejectionResponse {
        case httpResponse @ HttpResponse(
              _,
              _,
              httpEntity: HttpEntity.Strict,
              _
            ) =>
          // since all Akka default rejection responses are Strict this will handle all rejections
          val message = RejectionResult(httpEntity.data.utf8String)
          val jsonMessage = message.asJson.noSpaces
          httpResponse.withEntity(ContentTypes.`application/json`, jsonMessage)
        case httpResponse =>
          httpResponse // pass through all other types of responses
      }
  }

  val mapRejectionsToJson: Directive0 = {
    handleRejections(mapRejectionsToJsonHandler)
  }
}
