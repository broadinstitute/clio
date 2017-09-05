package org.broadinstitute.clio.client.webclient

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import io.circe.syntax._
import ClientAutoDerivation._
import org.broadinstitute.clio.client.util.FutureWithErrorMessage

import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshal}
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport
import io.circe.Printer
import org.broadinstitute.clio.transfer.model.{
  TransferWgsUbamV1Key,
  TransferWgsUbamV1Metadata,
  TransferWgsUbamV1QueryInput
}
import org.broadinstitute.clio.util.json.ModelAutoDerivation

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.runtime.universe.{typeOf, TypeTag}

class ClioWebClient(clioHost: String, clioPort: Int, useHttps: Boolean)(
  implicit system: ActorSystem
) extends FailFastCirceSupport
    with ModelAutoDerivation
    with FutureWithErrorMessage {
  implicit val executionContext: ExecutionContext = system.dispatcher
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  val QueueSize = 10

  private val connectionFlow = {
    if (useHttps) {
      Http().outgoingConnectionHttps(clioHost, clioPort)
    } else {
      Http().outgoingConnection(clioHost, clioPort)
    }
  }

  def dispatchRequest(request: HttpRequest): Future[HttpResponse] = {
    Source
      .single(request)
      .via(connectionFlow)
      .runWith(Sink.head)
      .logErrorMsg("Failed to send HTTP request to Clio server")
  }

  def getClioServerVersion: Future[HttpResponse] = {
    dispatchRequest(HttpRequest(uri = "/version"))
  }

  def getClioServerHealth: Future[HttpResponse] = {
    dispatchRequest(HttpRequest(uri = "/health"))
  }

  def getWgsUbamSchema(
    implicit bearerToken: OAuth2BearerToken
  ): Future[HttpResponse] = {
    dispatchRequest(
      HttpRequest(uri = "/api/v1/wgsubam/schema")
        .addHeader(Authorization(credentials = bearerToken))
    )
  }

  def addWgsUbam(
    input: TransferWgsUbamV1Key,
    transferWgsUbamV1Metadata: TransferWgsUbamV1Metadata
  )(implicit bearerToken: OAuth2BearerToken): Future[HttpResponse] = {
    val entity = HttpEntity(
      ContentTypes.`application/json`,
      transferWgsUbamV1Metadata.asJson.pretty(implicitly[Printer])
    )
    dispatchRequest(
      HttpRequest(
        uri = "/api/v1/wgsubam/metadata/"
          + input.flowcellBarcode + "/" + input.lane + "/" + input.libraryName + "/" + input.location,
        method = HttpMethods.POST,
        entity = entity
      ).addHeader(Authorization(credentials = bearerToken))
    )
  }

  def queryWgsUbam(
    input: TransferWgsUbamV1QueryInput
  )(implicit bearerToken: OAuth2BearerToken): Future[HttpResponse] = {
    val entity =
      HttpEntity(
        ContentTypes.`application/json`,
        input.asJson.pretty(implicitly[Printer])
      )
    dispatchRequest(
      HttpRequest(
        uri = "/api/v1/wgsubam/query",
        method = HttpMethods.POST,
        entity = entity
      ).addHeader(Authorization(credentials = bearerToken))
    )
  }

  def unmarshal[A: FromEntityUnmarshaller: TypeTag](
    httpResponse: HttpResponse
  ): Future[A] = {
    Unmarshal(httpResponse)
      .to[A]
      .logErrorMsg(
        s"Could not convert entity from $httpResponse to ${typeOf[A]}"
      )
  }

  def ensureOkResponse(httpResponse: HttpResponse): HttpResponse = {
    if (httpResponse.status.isSuccess()) {
      httpResponse
    } else {
      throw new Exception(
        s"Got an error from the Clio server. Status code: ${httpResponse.status}"
      )
    }
  }
}
