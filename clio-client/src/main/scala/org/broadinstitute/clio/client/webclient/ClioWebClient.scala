package org.broadinstitute.clio.client.webclient

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import io.circe.syntax._
import ClientAutoDerivation._
import akka.http.scaladsl.unmarshalling.Unmarshal
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport
import io.circe.Printer
import org.broadinstitute.clio.transfer.model.{TransferWgsUbamV1Key, TransferWgsUbamV1Metadata, TransferWgsUbamV1QueryInput, TransferWgsUbamV1QueryOutput}
import org.broadinstitute.clio.util.json.ModelAutoDerivation

import scala.concurrent.{ExecutionContext, Future}

class ClioWebClient(clioHost: String, clioPort: Int, useHttps: Boolean)(
  implicit system: ActorSystem
) extends ModelAutoDerivation
with ErrorAccumulatingCirceSupport {
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

  def getWgsUbamSchema(bearerToken: String): Future[HttpResponse] = {
    dispatchRequest(
      HttpRequest(uri = "/api/v1/wgsubam/schema")
        .addHeader(Authorization(credentials = OAuth2BearerToken(bearerToken)))
    )
  }

  def addWgsUbam(
    bearerToken: String,
    input: TransferWgsUbamV1Key,
    transferWgsUbamV1Metadata: TransferWgsUbamV1Metadata
  ): Future[HttpResponse] = {
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
      ).addHeader(Authorization(credentials = OAuth2BearerToken(bearerToken)))
    )
  }

  def queryWgsUbam(bearerToken: String,
                   input: TransferWgsUbamV1QueryInput): Future[HttpResponse] = {
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
      ).addHeader(Authorization(credentials = OAuth2BearerToken(bearerToken)))
    )
  }

  def getWgsUbamModels(bearerToken: String,
                       input: TransferWgsUbamV1QueryInput): Future[Seq[TransferWgsUbamV1QueryOutput]] = {
    queryWgsUbam(bearerToken, input).flatMap(Unmarshal(_).to[Seq[TransferWgsUbamV1QueryOutput]])
  }
}
