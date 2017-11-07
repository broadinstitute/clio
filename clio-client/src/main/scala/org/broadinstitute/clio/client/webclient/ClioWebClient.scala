package org.broadinstitute.clio.client.webclient

import java.time.Instant

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.settings.ConnectionPoolSettings
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshal}
import akka.stream._
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.google.auth.oauth2.OAuth2Credentials
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport
import io.circe.{Json, Printer}
import io.circe.syntax._
import org.broadinstitute.clio.client.util.FutureWithErrorMessage
import org.broadinstitute.clio.transfer.model._
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.UpsertId

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
    *
    * Uses one of Akka's cached host connection pools under-the-hood,
    * so repeated requests to the remove Clio server (i.e. during backfilling)
    * will reuse some of the HTTP system infrastructure instead of
    * setting up a new connection from scratch for each request.
    */
  private val connectionFlow = {
    val settings = {
      val default = ConnectionPoolSettings.default

      default
        .withConnectionSettings(
          default.connectionSettings.withIdleTimeout(requestTimeout)
        )
        .withMaxRetries(0)
        .withMaxConnections(maxConcurrentRequests)
        .withMaxOpenRequests(maxConcurrentRequests)
    }

    if (useHttps) {
      Http().cachedHostConnectionPoolHttps[Promise[HttpResponse]](
        clioHost,
        clioPort,
        settings = settings
      )
    } else {
      Http().cachedHostConnectionPool[Promise[HttpResponse]](
        clioHost,
        clioPort,
        settings = settings
      )
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
    .via(connectionFlow)
    .toMat(Sink.foreach {
      case (Success(response), promise) => promise.success(response)
      case (Failure(ex), promise)       => promise.failure(ex)
    })(Keep.left)
    .run()

  def dispatchRequest(
    request: HttpRequest,
    credentials: Option[OAuth2Credentials]
  ): Future[HttpResponse] = {
    val responsePromise = Promise[HttpResponse]()
    val requestWithCreds = credentials.fold(request) { creds =>
      val now = Instant.now()

      creds.synchronized {
        /*
         * Refresh the credential if either:
         *   1. It has no underlying access token, or
         *   2. The underlying access token has an expiration time, and it's expired
         *
         * DON'T refresh if the underlying access token has no expiration time,
         * because refresh might not be supported for that token.
         */
        val maybeToken = Option(creds.getAccessToken)
        lazy val maybeTokenExpirationInstant = for {
          token <- maybeToken
          expiration <- Option(token.getExpirationTime)
        } yield {
          expiration.toInstant
        }
        val shouldRefresh =
          maybeToken.isEmpty || maybeTokenExpirationInstant.exists(
            _.isAfter(now)
          )

        if (shouldRefresh) {
          creds.refresh()
        }
      }

      request.addCredentials(
        OAuth2BearerToken(creds.getAccessToken.getTokenValue)
      )
    }

    requestStream.offer(requestWithCreds -> responsePromise).flatMap {
      queueResult =>
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

  def getClioServerVersion: Future[Json] = {
    dispatchRequest(HttpRequest(uri = "/version"), None)
      .flatMap(unmarshal[Json])
  }

  def getClioServerHealth: Future[Json] = {
    dispatchRequest(HttpRequest(uri = "/health"), None)
      .flatMap(unmarshal[Json])
  }

  def getSchema(
    transferIndex: TransferIndex
  )(implicit credentials: OAuth2Credentials): Future[Json] = {
    dispatchRequest(
      HttpRequest(uri = s"/api/v1/${transferIndex.urlSegment}/schema"),
      Some(credentials)
    ).flatMap(unmarshal[Json])
  }

  def upsert[TI <: TransferIndex](transferIndex: TI)(
    key: transferIndex.KeyType,
    metadata: transferIndex.MetadataType
  )(implicit credentials: OAuth2Credentials): Future[UpsertId] = {
    import transferIndex.implicits._

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
      ),
      Some(credentials)
    ).flatMap(unmarshal[UpsertId])
  }

  def query[TI <: TransferIndex](transferIndex: TI)(
    input: transferIndex.QueryInputType,
    includeDeleted: Boolean
  )(implicit credentials: OAuth2Credentials): Future[Json] = {
    import transferIndex.implicits._

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
      ),
      Some(credentials)
    ).flatMap(unmarshal[Json])
  }

  private def unmarshal[A: FromEntityUnmarshaller: TypeTag](
    httpResponse: HttpResponse
  ): Future[A] = {
    Unmarshal(httpResponse)
      .to[A]
      .logErrorMsg(
        s"Could not convert entity from $httpResponse to ${typeOf[A]}"
      )
  }

  private def ensureOkResponse(
    httpResponse: HttpResponse
  ): Future[HttpResponse] = {
    if (httpResponse.status.isSuccess()) {
      logger.info(
        s"Successfully completed command. Response code: ${httpResponse.status}"
      )
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
