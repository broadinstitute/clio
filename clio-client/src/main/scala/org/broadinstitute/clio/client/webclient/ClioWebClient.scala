package org.broadinstitute.clio.client.webclient

import org.broadinstitute.clio.client.util.FutureWithErrorMessage
import org.broadinstitute.clio.transfer.model._

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshal}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport
import io.circe.Printer
import io.circe.syntax._

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.runtime.universe.{TypeTag, typeOf}

class ClioWebClient(
  clioHost: String,
  clioPort: Int,
  useHttps: Boolean,
  requestTimeout: FiniteDuration
)(implicit system: ActorSystem)
    extends FailFastCirceSupport
    with FutureWithErrorMessage {

  import ClientAutoDerivation._

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
      .completionTimeout(requestTimeout)
      .runWith(Sink.head)
      .logErrorMsg("Failed to send HTTP request to Clio server")
  }

  def getClioServerVersion: Future[HttpResponse] = {
    dispatchRequest(HttpRequest(uri = "/version"))
  }

  def getClioServerHealth: Future[HttpResponse] = {
    dispatchRequest(HttpRequest(uri = "/health"))
  }

  def getSchemaWgsUbam(
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

  private def query(
    index: String,
    input: RequestEntity,
    includeDeleted: Boolean
  )(implicit bearerToken: OAuth2BearerToken): Future[HttpResponse] = {
    val queryPath = if (includeDeleted) "queryall" else "query"

    dispatchRequest(
      HttpRequest(
        uri = s"/api/v1/$index/$queryPath",
        method = HttpMethods.POST,
        entity = input
      ).addHeader(Authorization(credentials = bearerToken))
    )
  }

  def queryWgsUbam(input: TransferWgsUbamV1QueryInput, includeDeleted: Boolean)(
    implicit bearerToken: OAuth2BearerToken
  ): Future[HttpResponse] = {
    val entity =
      HttpEntity(
        ContentTypes.`application/json`,
        input.asJson.pretty(implicitly[Printer])
      )
    query("wgsubam", entity, includeDeleted)
  }

  def getSchemaGvcf(
    implicit bearerToken: OAuth2BearerToken
  ): Future[HttpResponse] = {
    dispatchRequest(
      HttpRequest(uri = "/api/v1/gvcf/schema")
        .addHeader(Authorization(credentials = bearerToken))
    )
  }

  def addGvcf(
    input: TransferGvcfV1Key,
    transferGvcfV1Metadata: TransferGvcfV1Metadata
  )(implicit bearerToken: OAuth2BearerToken): Future[HttpResponse] = {
    val entity = HttpEntity(
      ContentTypes.`application/json`,
      transferGvcfV1Metadata.asJson.pretty(implicitly[Printer])
    )
    dispatchRequest(
      HttpRequest(
        uri = "/api/v1/gvcf/metadata/"
          + input.location + '/'
          + input.project + '/'
          + input.sampleAlias + '/'
          + input.version,
        method = HttpMethods.POST,
        entity = entity
      ).addHeader(Authorization(credentials = bearerToken))
    )
  }

  def queryGvcf(input: TransferGvcfV1QueryInput, includeDeleted: Boolean)(
    implicit bearerToken: OAuth2BearerToken
  ): Future[HttpResponse] = {
    val entity =
      HttpEntity(
        ContentTypes.`application/json`,
        input.asJson.pretty(implicitly[Printer])
      )
    query("gvcf", entity, includeDeleted)
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
