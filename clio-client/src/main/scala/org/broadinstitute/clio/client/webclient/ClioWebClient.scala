package org.broadinstitute.clio.client.webclient

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Authorization, HttpCredentials}
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshal}
import akka.stream._
import akka.stream.scaladsl.{Keep, Sink, Source}
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport
import io.circe.{Encoder, Printer}
import io.circe.syntax._
import org.broadinstitute.clio.client.util.FutureWithErrorMessage
import org.broadinstitute.clio.transfer.model._
import org.broadinstitute.clio.util.json.ModelAutoDerivation

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.reflect.runtime.universe.{TypeTag, typeOf}
import scala.util.{Failure, Success}

class ClioWebClient(
  clioHost: String,
  clioPort: Int,
  useHttps: Boolean,
  maxQueuedRequests: Int,
  maxConcurrentRequests: Int,
  requestTimeout: FiniteDuration
)(implicit system: ActorSystem)
    extends ModelAutoDerivation
    with FailFastCirceSupport
    with FutureWithErrorMessage {

  implicit val executionContext: ExecutionContext = system.dispatcher
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  /**
    * Reusable stream component ingesting `HttpRequest`s and emitting
    * `HttpResponse`s by communicating with the Clio server.
    */
  private val connectionFlow = {
    if (useHttps) {
      Http().outgoingConnectionHttps(clioHost, clioPort)
    } else {
      Http().outgoingConnection(clioHost, clioPort)
    }
  }

  /**
    * Long-running HTTP request stream handling communications with the Clio server.
    *
    * Calls to the client will result in new HTTP requests being pushed onto this queue,
    * paired with the Promise that should be completed with the corresponding HTTP response.
    *
    * As downstream resources are made available, requests will be pulled off the
    * queue and sent to the Clio server. When a response is returned, the Promise will
    * be completed with a corresponding Success / Failure.
    *
    * Based on the example at:
    * https://doc.akka.io/docs/akka-http/current/scala/http/client-side/host-level.html#using-the-host-level-api-with-a-queue
    */
  private val requestStream = Source
    .queue[(HttpRequest, Promise[HttpResponse])](
      maxQueuedRequests,
      OverflowStrategy.dropNew
    )
    .mapAsyncUnordered(maxConcurrentRequests) {
      case (request, promise) => {
        /*
         * Stream-within-a-stream is weird, but Akka HTTP doesn't currently
         * support a proper client-side request timeout when using their
         * higher-level cached APIs.
         */
        val response = Source
          .single(request)
          .via(connectionFlow)
          .completionTimeout(requestTimeout)
          .runWith(Sink.head)

        response.transformWith {
          case Success(r)  => Future.successful(Success(r) -> promise)
          case Failure(ex) => Future.successful(Failure(ex) -> promise)
        }
      }
    }
    .toMat(Sink.foreach {
      case (Success(response), promise) => promise.success(response)
      case (Failure(ex), promise)       => promise.failure(ex)
    })(Keep.left)
    .run()

  def dispatchRequest(request: HttpRequest): Future[HttpResponse] = {
    val responsePromise = Promise[HttpResponse]()
    requestStream.offer(request -> responsePromise).flatMap { queueResult =>
      queueResult match {
        case QueueOfferResult.Enqueued => ()
        case QueueOfferResult.Dropped => {
          responsePromise.failure {
            new RuntimeException(
              s"Client queue was too full to add the request: $request"
            )
          }
        }
        case QueueOfferResult.Failure(ex) =>
          responsePromise.failure(ex)
        case QueueOfferResult.QueueClosed =>
          responsePromise.failure {
            new RuntimeException(
              s"Client queue was closed while adding the request: $request"
            )
          }
      }

      responsePromise.future
        .logErrorMsg("Failed to send HTTP request to Clio server")
        .flatMap(ensureOkResponse)
    }
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
    /*
     * The `/` method on Uri.Path performs a raw URI encoding on the
     * added segment, which is needed to deal with potential spaces
     * in the fields of the key.
     */
    val encodedPath = key.getUrlSegments.foldLeft(
      Uri.Path(s"/api/v1/${transferIndex.urlSegment}/metadata")
    )(_ / _)

    dispatchRequest(
      HttpRequest(
        uri = Uri(path = encodedPath),
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

  def ensureOkResponse(httpResponse: HttpResponse): Future[HttpResponse] = {
    if (httpResponse.status.isSuccess()) {
      Future.successful(httpResponse)
    } else {
      httpResponse.entity.toStrict(requestTimeout).map { entity =>
        throw ClioWebClient.FailedResponse(httpResponse.status, entity)
      }
    }
  }
}

object ClioWebClient {
  case class FailedResponse(statusCode: StatusCode, entity: HttpEntity.Strict)
      extends RuntimeException(
        s"Got an error from the Clio server. Status code: $statusCode. Entity: $entity"
      )
}
