package org.broadinstitute.clio.client.webclient

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import io.circe.syntax._
import ClientAutoDerivation._
import io.circe.Printer
import org.broadinstitute.clio.transfer.model.{
  TransferReadGroupV1Key,
  TransferReadGroupV1Metadata,
  TransferReadGroupV1QueryInput
}

import scala.concurrent.{ExecutionContext, Future}

class ClioWebClient(clioHost: String, clioPort: Int, useHttps: Boolean)(
  implicit system: ActorSystem
) {
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
  }
  def getClioServerVersion: Future[HttpResponse] = {
    dispatchRequest(HttpRequest(uri = "/version"))
  }

  def getClioServerHealth: Future[HttpResponse] = {
    dispatchRequest(HttpRequest(uri = "/health"))
  }

  def getReadGroupSchema(bearerToken: String): Future[HttpResponse] = {
    dispatchRequest(
      HttpRequest(uri = "/api/v1/readgroup/schema")
        .addHeader(Authorization(credentials = OAuth2BearerToken(bearerToken)))
    )
  }

  def addReadGroupBam(
    bearerToken: String,
    input: TransferReadGroupV1Key,
    transferReadGroupV1Metadata: TransferReadGroupV1Metadata
  ): Future[HttpResponse] = {
    val entity = HttpEntity(
      ContentTypes.`application/json`,
      transferReadGroupV1Metadata.asJson.pretty(implicitly[Printer])
    )
    dispatchRequest(
      HttpRequest(
        uri = "/api/v1/readgroup/metadata/"
          + input.flowcellBarcode + "/" + input.lane + "/" + input.libraryName + "/" + input.location,
        method = HttpMethods.POST,
        entity = entity
      ).addHeader(Authorization(credentials = OAuth2BearerToken(bearerToken)))
    )
  }

  def queryReadGroupBam(
    bearerToken: String,
    input: TransferReadGroupV1QueryInput
  ): Future[HttpResponse] = {
    val entity =
      HttpEntity(
        ContentTypes.`application/json`,
        input.asJson.pretty(implicitly[Printer])
      )
    dispatchRequest(
      HttpRequest(
        uri = "/api/v1/readgroup/query/",
        method = HttpMethods.POST,
        entity = entity
      ).addHeader(Authorization(credentials = OAuth2BearerToken(bearerToken)))
    )
  }
}
