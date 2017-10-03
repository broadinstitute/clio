package org.broadinstitute.clio.client.webclient

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Authorization, HttpCredentials}
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshal}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport
import io.circe.{Encoder, Printer}
import io.circe.syntax._
import org.broadinstitute.clio.client.util.FutureWithErrorMessage
import org.broadinstitute.clio.transfer.model._

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

  def getSchema(
    transferIndex: TransferIndex
  )(implicit credentials: HttpCredentials): Future[HttpResponse] = {
    dispatchRequest(
      HttpRequest(uri = s"/api/v1/${transferIndex.urlSegment}/schema")
        .addHeader(Authorization(credentials = credentials))
    )
  }

  def upsert[M <: TransferMetadata[M]](
    transferIndex: TransferIndex,
    key: TransferKey,
    metadata: M
  )(implicit credentials: HttpCredentials,
    encoder: Encoder[M]): Future[HttpResponse] = {
    val entity = HttpEntity(
      ContentTypes.`application/json`,
      metadata.asJson.pretty(implicitly[Printer])
    )
    dispatchRequest(
      HttpRequest(
        uri = s"/api/v1/${transferIndex.urlSegment}/metadata/${key.getUrlPath}",
        method = HttpMethods.POST,
        entity = entity
      ).addHeader(Authorization(credentials = credentials))
    )
  }

  def query[T](transferIndex: TransferIndex, input: T, includeDeleted: Boolean)(
    implicit credentials: HttpCredentials,
    encoder: Encoder[T]
  ): Future[HttpResponse] = {
    val queryPath = if (includeDeleted) "queryall" else "query"

    val entity = HttpEntity(
      ContentTypes.`application/json`,
      input.asJson.pretty(implicitly[Printer])
    )
    dispatchRequest(
      HttpRequest(
        uri = s"/api/v1/${transferIndex.urlSegment}/$queryPath",
        method = HttpMethods.POST,
        entity = entity
      ).addHeader(Authorization(credentials = credentials))
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
