package org.broadinstitute.clio.server.webservice

import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import io.circe.generic.auto._
import io.circe.syntax._
import org.broadinstitute.clio.model.RejectionResult

/** Rejection translation into json. */
trait RejectionDirectives {
  lazy val mapRejectionsToJson: Directive0 = {
    handleRejections(mapRejectionsToJsonHandler)
  }

  private val mapRejectionsToJsonHandler: RejectionHandler = {
    RejectionHandler.default.mapRejectionResponse {
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
}
