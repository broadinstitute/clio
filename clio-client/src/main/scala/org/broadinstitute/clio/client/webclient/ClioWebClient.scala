package org.broadinstitute.clio.client.webclient

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.scaladsl.{Flow, JsonFraming, Source}
import akka.util.ByteString
import com.typesafe.scalalogging.StrictLogging
import io.circe.jawn.JawnParser
import io.circe.syntax._
import io.circe.{Json, Printer}
import org.broadinstitute.clio.transfer.model._
import org.broadinstitute.clio.util.generic.{CirceEquivalentCamelCaseLexer, FieldMapper}
import org.broadinstitute.clio.util.json.ModelAutoDerivation

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

class ClioWebClient(
  clioHost: String,
  clioPort: Int,
  useHttps: Boolean,
  requestTimeout: FiniteDuration,
  maxRequestRetries: Int,
  tokenGenerator: CredentialsGenerator
)(implicit system: ActorSystem)
    extends ModelAutoDerivation
    with StrictLogging {

  import ApiConstants._

  implicit val executionContext: ExecutionContext = system.dispatcher

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

  private val parserFlow = {
    val parser = new JawnParser
    Flow.fromFunction[ByteString, Json] { bytes =>
      parser.parseByteBuffer(bytes.asByteBuffer).fold(throw _, identity)
    }
  }

  def dispatchRequest(
    request: HttpRequest,
    includeAuth: Boolean = true,
  ): Source[ByteString, NotUsed] = {
    val requestWithCreds = if (includeAuth) {
      request.addCredentials(tokenGenerator.generateCredentials())
    } else {
      request
    }

    /*
     * Reusable `Source` which will run the given `HttpRequest` over the connection
     * flow to the Clio server, erroring out early if the request fails to complete
     * within the request timeout.
     */
    val responseSource = Source
      .single(requestWithCreds)
      .via(connectionFlow)
      .completionTimeout(requestTimeout)

    /*
     * Retry on any connection failures (since Akka's built-in retry mechanisms
     * only trigger on idempotent verbs like GET).
     *
     * Every retry will produce a new "materialization" of `responseSource`, so
     * even though it's defined as a `Source.single` we can retry with it as many
     * times as we need.
     */
    val retriedResponse = responseSource
      .recoverWithRetries(maxRequestRetries, {
        case _ => responseSource
      })

    retriedResponse.flatMapConcat { response =>
      if (response.status.isSuccess()) {
        logger.debug(s"Successfully completed request: $request")
        response.entity.dataBytes
      } else {
        response.entity.dataBytes.reduce(_ ++ _).flatMapConcat { entity =>
          Source.failed {
            ClioWebClient.FailedResponse(
              response.status,
              HttpEntity.Strict(response.entity.contentType, entity)
            )
          }
        }
      }
    }
  }

  def getClioServerVersion: Source[Json, NotUsed] =
    dispatchRequest(HttpRequest(uri = s"/$versionString"), includeAuth = false)
      .via(parserFlow)

  def getClioServerHealth: Source[Json, NotUsed] =
    dispatchRequest(HttpRequest(uri = s"/$healthString"), includeAuth = false)
      .via(parserFlow)

  def upsert[CI <: ClioIndex](clioIndex: CI)(
    key: clioIndex.KeyType,
    metadata: clioIndex.MetadataType,
    force: Boolean = false
  ): Source[Json, NotUsed] = {

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
      Uri.Path(s"/$apiString/v1/${clioIndex.urlSegment}/$metadataString")
    )(_ / _)

    dispatchRequest(
      HttpRequest(
        uri = Uri(path = encodedPath).withQuery(Uri.Query(forceString -> force.toString)),
        method = HttpMethods.POST,
        entity = entity
      )
    ).via(parserFlow)
  }

  def query[CI <: ClioIndex](clioIndex: CI)(
    input: clioIndex.QueryInputType,
    includeDeleted: Boolean
  ): Source[Json, NotUsed] = {
    import clioIndex.implicits._
    rawQuery(clioIndex)(input.asJson, includeDeleted)
  }

  def getMetadataForKey[CI <: ClioIndex](clioIndex: CI)(
    input: clioIndex.KeyType,
    includeDeleted: Boolean
  ): Source[clioIndex.MetadataType, NotUsed] = {

    import clioIndex.implicits._
    import s_mach.string._

    val keyJson = input.asJson
    val keyFields = FieldMapper[clioIndex.KeyType].fields.keySet
      .map(_.toSnakeCase(CirceEquivalentCamelCaseLexer))

    rawQuery(clioIndex)(keyJson, includeDeleted)
      .fold[Either[Throwable, Option[clioIndex.MetadataType]]](Right(None)) {
        (acc, json) =>
          acc match {
            case Right(None) =>
              json
                .mapObject(_.filterKeys(!keyFields.contains(_)))
                .as[clioIndex.MetadataType]
                .map(Some(_))
            case Right(Some(_)) =>
              Left(
                new IllegalStateException(
                  s"""Got > 1 ${clioIndex.name}s from Clio for key:
                     |${keyJson.spaces2}""".stripMargin
                )
              )
            case _ => acc
          }
      }
      .flatMapConcat {
        _.fold(
          Source.failed,
          // Writing the 'Some' case as just 'Source.single' runs afoul of Scala's
          // restriction on dependently-typed function values.
          _.fold(Source.empty[clioIndex.MetadataType])(m => Source.single(m))
        )
      }
  }

  private[webclient] def rawQuery(clioIndex: ClioIndex)(
    input: Json,
    includeDeleted: Boolean
  ): Source[Json, NotUsed] = {
    val queryPath = if (includeDeleted) queryAllString else queryString

    val entity = HttpEntity(
      ContentTypes.`application/json`,
      input.pretty(implicitly[Printer])
    )
    dispatchRequest(
      HttpRequest(
        uri = s"/$apiString/v1/${clioIndex.urlSegment}/$queryPath",
        method = HttpMethods.POST,
        entity = entity
      )
    ).via(JsonFraming.objectScanner(Int.MaxValue)).via(parserFlow)
  }
}

object ClioWebClient {
  case class FailedResponse(statusCode: StatusCode, entity: HttpEntity.Strict)
      extends RuntimeException(
        s"Got an error from the Clio server. Status code: $statusCode. Entity: $entity"
      )
}
