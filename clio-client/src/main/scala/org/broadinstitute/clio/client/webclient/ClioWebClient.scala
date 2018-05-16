package org.broadinstitute.clio.client.webclient

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import better.files.File
import io.circe.parser._
import akka.stream.scaladsl.{Flow, JsonFraming, Source}
import akka.util.ByteString
import com.typesafe.scalalogging.StrictLogging
import io.circe.jawn.JawnParser
import io.circe.syntax._
import io.circe.Json
import org.broadinstitute.clio.client.ClioClientConfig
import org.broadinstitute.clio.transfer.model._
import org.broadinstitute.clio.util.auth.ClioCredentials
import org.broadinstitute.clio.util.generic.{CirceEquivalentCamelCaseLexer, FieldMapper}
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.DocumentStatus

import scala.concurrent.duration.FiniteDuration

object ClioWebClient {

  type UpsertAux[K, M] = ClioIndex { type KeyType = K; type MetadataType = M }
  type QueryAux[I] = ClioIndex { type QueryInputType = I }

  def apply(
    credentials: ClioCredentials,
    clioHost: String = ClioClientConfig.ClioServer.clioServerHostName,
    clioPort: Int = ClioClientConfig.ClioServer.clioServerPort,
    useHttps: Boolean = ClioClientConfig.ClioServer.clioServerUseHttps,
    requestTimeout: FiniteDuration = ClioClientConfig.responseTimeout,
    maxRequestRetries: Int = ClioClientConfig.maxRequestRetries
  )(implicit system: ActorSystem): ClioWebClient = {

    /*
     * We use the "low-level" connection API from Akka HTTP instead
     * of the higher-level "cached host connection pool" API because of a
     * race condition in the pool implementation that can cause successful
     * requests to be reported as failures:
     *
     * https://github.com/akka/akka-http/issues/1459#issuecomment-335487195
     */
    val connectionFlow = {
      if (useHttps) {
        Http().outgoingConnectionHttps(clioHost, clioPort)
      } else {
        Http().outgoingConnection(clioHost, clioPort)
      }
    }

    new ClioWebClient(
      connectionFlow,
      requestTimeout,
      maxRequestRetries,
      GoogleCredentialsGenerator(credentials)
    )
  }

  case class FailedResponse(statusCode: StatusCode, entityBody: String)
      extends RuntimeException(
        s"Got an error from the Clio server. Status code: $statusCode. Entity: $entityBody"
      )
}

class ClioWebClient(
  connectionFlow: Flow[HttpRequest, HttpResponse, _],
  requestTimeout: FiniteDuration,
  maxRequestRetries: Int,
  tokenGenerator: CredentialsGenerator
) extends StrictLogging {

  import ApiConstants._

  private val parserFlow = {
    val parser = new JawnParser
    Flow.fromFunction[ByteString, Json] { bytes =>
      parser.parseByteBuffer(bytes.asByteBuffer).fold(throw _, identity)
    }
  }

  def dispatchRequest(
    request: HttpRequest,
    includeAuth: Boolean,
  ): Source[ByteString, NotUsed] = {
    val requestWithCreds = if (includeAuth) {
      request.addCredentials(tokenGenerator.generateCredentials())
    } else {
      request
    }

    /*
     * Reusable `Source` which will run the given `HttpRequest` over the connection
     * flow to the Clio server, erroring out early if the response fails to arrive
     * within the request timeout.
     */
    val responseSource = Source
      .single(requestWithCreds)
      .via(connectionFlow)
      .initialTimeout(requestTimeout)

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
        response.entity.withoutSizeLimit().dataBytes.idleTimeout(requestTimeout)
      } else {
        response.entity.dataBytes.reduce(_ ++ _).flatMapConcat { bytes =>
          Source.failed {
            ClioWebClient.FailedResponse(
              response.status,
              bytes.decodeString(
                response.entity.contentType.charsetOption
                  .getOrElse(HttpCharsets.`UTF-8`)
                  .value
              )
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

  def upsert[K <: IndexKey, M](clioIndex: ClioWebClient.UpsertAux[K, M])(
    key: K,
    metadata: M,
    force: Boolean = false
  ): Source[Json, NotUsed] = {

    import clioIndex.implicits._

    val entity = HttpEntity(
      ContentTypes.`application/json`,
      metadata.asJson.pretty(ModelAutoDerivation.defaultPrinter)
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
      ),
      includeAuth = true
    ).via(parserFlow)
  }

  def simpleQuery[I](clioIndex: ClioWebClient.QueryAux[I])(
    input: I,
    includeDeleted: Boolean
  ): Source[Json, NotUsed] = {
    import clioIndex.implicits._
    if (includeDeleted) {
      query(clioIndex)(input.asJson)
    } else {
      query(clioIndex)(
        Metadata.jsonWithDocumentStatus(
          input.asJson,
          DocumentStatus.Normal
        )
      )
    }
  }

  def jsonFileQuery[CI <: ClioIndex](clioIndex: CI)(
    input: File
  ): Source[Json, NotUsed] = {
    parse(input.contentAsString)
      .fold(
        e =>
          Source.failed(
            new IllegalArgumentException(
              s"Input file at ${input.path.toString} must contain valid Json.",
              e
            )
        ),
        query(clioIndex)(_, raw = true)
      )
  }

  def getMetadataForKey[K, M](clioIndex: ClioWebClient.UpsertAux[K, M])(
    input: K,
    includeDeleted: Boolean
  ): Source[M, NotUsed] = {

    import clioIndex.implicits._
    import s_mach.string._

    val keyJson = if (includeDeleted) {
      input.asJson
    } else {
      Metadata.jsonWithDocumentStatus(
        input.asJson,
        DocumentStatus.Normal
      )
    }
    val keyFields = FieldMapper[K].fields.keySet
      .map(_.toSnakeCase(CirceEquivalentCamelCaseLexer))

    query(clioIndex)(keyJson)
      .fold[Either[Throwable, Option[M]]](Right(None)) { (acc, json) =>
        acc match {
          case Right(None) =>
            json
              .mapObject(_.filterKeys(!keyFields.contains(_)))
              .as[M]
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
          _.fold(Source.empty[M])(Source.single)
        )
      }
  }

  private[client] def query(clioIndex: ClioIndex)(
    input: Json,
    raw: Boolean = false
  ): Source[Json, NotUsed] = {
    val queryPath = if (raw) rawQueryString else queryString
    val entity = HttpEntity(
      ContentTypes.`application/json`,
      input.pretty(ModelAutoDerivation.defaultPrinter)
    )
    dispatchRequest(
      HttpRequest(
        uri = s"/$apiString/v1/${clioIndex.urlSegment}/$queryPath",
        method = HttpMethods.POST,
        entity = entity
      ),
      includeAuth = true
    ).via(JsonFraming.objectScanner(Int.MaxValue)).via(parserFlow)
  }
}
