package org.broadinstitute.clio.client.webclient

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshal}
import akka.stream._
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.typesafe.scalalogging.StrictLogging
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport
import io.circe.{Json, Printer}
import io.circe.syntax._
import org.broadinstitute.clio.transfer.model._
import org.broadinstitute.clio.util.ClassUtil
import org.broadinstitute.clio.util.generic.{CaseClassMapper, CaseClassTypeConverter}
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
  requestTimeout: FiniteDuration,
  maxRequestRetries: Int,
  tokenGenerator: CredentialsGenerator
)(implicit system: ActorSystem)
    extends ModelAutoDerivation
    with FailFastCirceSupport
    with StrictLogging {

  implicit val executionContext: ExecutionContext = system.dispatcher
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  /**
    * FIXME: We use the "low-level" connection API from Akka HTTP instead
    * of the higher-level "cached host connection pool" API because of a
    * race condition in the pool implementation that can cause successful
    * requests to be reported as failures:
    *
    * https://github.com/akka/akka-http/issues/1459#issuecomment-335487195
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
         * Reusable `Source` which will run the given `HttpRequest` over the connection
         * flow to the Clio server, erroring out early if the request fails to complete
         * within the request timeout.
         */
        val responseSource = Source
          .single(request)
          .via(connectionFlow)
          .completionTimeout(requestTimeout)

        /*
         * Run the source with a `Sink` that takes the first element, retrying on any
         * connection failures.
         *
         * Every retry will produce a new "materialization" of `responseSource`, so
         * even though it's defined as a `Source.single` we can retry with it as many
         * times as we need.
         */
        val response = responseSource
          .recoverWithRetries(maxRequestRetries, {
            case _ => responseSource
          })
          .runWith(Sink.head)

        response.andThen {
          /*
           * Side-effect on the result of the HTTP request to communicate the result
           * back to the caller.
           *
           * Note: `Failure` here means a failure to send the request / process the
           * result, not a failure by the server. `Success` might contain an `HttpResponse`
           * with an error status code, and it's up to the caller to handle that.
           */
          case Success(r)  => promise.success(r)
          case Failure(ex) => promise.failure(ex)
        }.transform { _ =>
          /*
           * Always return a `Success` to prevent the stream from closing on failures.
           * The `andThen` above handles communicating the actual result back to the
           * caller by side-effecting on the promise.
           */
          Success(Done)
        }
      }
    }
    /*
     * Since we communicate `HttpResponse`s back to the caller via side-effecting on
     * promises, we ignore the outputs of the stream here.
     *
     * `Keep.left` sets the output of this expression to be the input `Source.queue`
     * instead of the output `akka.Done`.
     */
    .toMat(Sink.ignore)(Keep.left)
    .run()

  def dispatchRequest(
    request: HttpRequest,
    includeAuth: Boolean = true
  ): Future[HttpResponse] = {
    val responsePromise = Promise[HttpResponse]()
    val requestWithCreds = if (includeAuth) {
      request.addCredentials(tokenGenerator.generateCredentials())
    } else {
      request
    }

    requestStream.offer(requestWithCreds -> responsePromise).flatMap { queueResult =>
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

      responsePromise.future.flatMap(ensureOkResponse)
    }
  }

  def getClioServerVersion: Future[Json] = {
    dispatchRequest(HttpRequest(uri = "/version"), includeAuth = false)
      .flatMap(unmarshal[Json])
  }

  def getClioServerHealth: Future[Json] = {
    dispatchRequest(HttpRequest(uri = "/health"), includeAuth = false)
      .flatMap(unmarshal[Json])
  }

  def getSchema(clioIndex: ClioIndex): Future[Json] = {
    dispatchRequest(
      HttpRequest(uri = s"/api/v1/${clioIndex.urlSegment}/schema")
    ).flatMap(unmarshal[Json])
  }

  def upsert[CI <: ClioIndex](clioIndex: CI)(
    key: clioIndex.KeyType,
    metadata: clioIndex.type#MetadataType
  ): Future[UpsertId] = {
    import clioIndex.implicits._

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
      Uri.Path(s"/api/v1/${clioIndex.urlSegment}/metadata")
    )(_ / _)

    dispatchRequest(
      HttpRequest(
        uri = Uri(path = encodedPath),
        method = HttpMethods.POST,
        entity = entity
      )
    ).flatMap(unmarshal[UpsertId])
  }

  def query[CI <: ClioIndex](clioIndex: CI)(
    input: clioIndex.QueryInputType,
    includeDeleted: Boolean
  ): Future[Json] = {
    import clioIndex.implicits._

    val queryPath = if (includeDeleted) "queryall" else "query"

    val entity = HttpEntity(
      ContentTypes.`application/json`,
      input.asJson.pretty(implicitly[Printer])
    )
    dispatchRequest(
      HttpRequest(
        uri = s"/api/v1/${clioIndex.urlSegment}/$queryPath",
        method = HttpMethods.POST,
        entity = entity
      )
    ).flatMap(unmarshal[Json])
  }

  def getMetadataForKey[CI <: ClioIndex](clioIndex: CI)(
    input: clioIndex.KeyType
  ): Future[Option[clioIndex.type#MetadataType]] = {
    import clioIndex.implicits._

    val keyFields = new CaseClassMapper[clioIndex.KeyType].names

    val keyToQueryMapper = CaseClassTypeConverter[
      clioIndex.KeyType,
      clioIndex.QueryInputType
    ](_.mapValues(Option.apply[Any]))

    val outputToMetadataMapper =
      CaseClassTypeConverter[
        clioIndex.QueryOutputType,
        clioIndex.MetadataType
      ](_ -- keyFields)

    query(clioIndex)(keyToQueryMapper.convert(input), includeDeleted = false)
      .map(_.as[Seq[Json]].fold(throw _, identity).toList)
      .map {
        case Nil => None
        case js :: Nil => {
          val output = js.as[clioIndex.QueryOutputType].fold(throw _, identity)
          Some(outputToMetadataMapper.convert(output))
        }
        case many => {
          val prettyKey = ClassUtil.formatFields(input)
          throw new IllegalStateException(
            s"""Got > 1 ${clioIndex.name}s from Clio for $prettyKey:
               |${many.asJson.pretty(Printer.spaces2)}""".stripMargin
          )
        }
      }
  }

  private def unmarshal[A: FromEntityUnmarshaller: TypeTag](
    httpResponse: HttpResponse
  ): Future[A] = {
    Unmarshal(httpResponse)
      .to[A]
      .recover {
        case ex =>
          throw new RuntimeException(
            s"Could not convert entity from $httpResponse to ${typeOf[A]}",
            ex
          )
      }
  }

  private def ensureOkResponse(
    httpResponse: HttpResponse
  ): Future[HttpResponse] = {
    if (httpResponse.status.isSuccess()) {
      logger.debug(
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
