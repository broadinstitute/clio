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
import io.circe.Json
import org.broadinstitute.clio.client.ClioClientConfig
import org.broadinstitute.clio.transfer.model._
import org.broadinstitute.clio.util.auth.ClioCredentials
import org.broadinstitute.clio.util.generic.{CirceEquivalentCamelCaseLexer, FieldMapper}
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.DocumentStatus

import scala.concurrent.duration.FiniteDuration

object ClioWebClient {

  /**
    * Type alias for a [[ClioIndex]] which extracts its type members for `Key`
    * and `Metadata` into type-parameter position.
    *
    * Useful for situations where dependent types confuse the compiler / macros.
    */
  type UpsertAux[K, M] = ClioIndex { type KeyType = K; type MetadataType = M }

  /**
    * Type alias for a [[ClioIndex]] which extracts its type member for `QueryInput`
    * into type-parameter position.
    *
    * Useful for situations where dependent types confuse the compiler / macros.
    */
  type QueryAux[I] = ClioIndex { type QueryInputType = I }

  /**
    * Build a [[ClioWebClient]] which will use the given credentials, along with
    * any overrides for settings managing connection / retry logic.
    */
  def apply(
    credentials: ClioCredentials,
    clioHost: String = ClioClientConfig.ClioServer.clioServerHostName,
    clioPort: Int = ClioClientConfig.ClioServer.clioServerPort,
    useHttps: Boolean = ClioClientConfig.ClioServer.clioServerUseHttps,
    responseTimeout: FiniteDuration = ClioClientConfig.responseTimeout,
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
      responseTimeout,
      maxRequestRetries,
      GoogleCredentialsGenerator(credentials)
    )
  }

  /** Custom exception for wrapping error responses from the Clio server. */
  case class FailedResponse(statusCode: StatusCode, entityBody: String)
      extends RuntimeException(
        s"Got an error from the Clio server. Status code: $statusCode. Entity: $entityBody"
      )
}

/**
  * A client for sending HTTP requests to a remote Clio server.
  *
  * Handles building requests (with auth credentials for endpoints that need it)
  * and parsing JSON responses. Retries requests that fail on connection errors
  * in case of a spurious network flap, and terminates requests that fail to
  * produce a timely response.
  */
class ClioWebClient(
  connectionFlow: Flow[HttpRequest, HttpResponse, _],
  responseTimeout: FiniteDuration,
  maxRequestRetries: Int,
  tokenGenerator: CredentialsGenerator
) extends StrictLogging {

  import ApiConstants._

  /**
    * Reusable stream component for parsing JSON elements from the byte stream of
    * a response from the Clio server.
    *
    * Relies on an upstream stage to chunk the flow of bytes such that each element
    * passing through this stage is parse-able as a single JSON element.
    */
  private val parserFlow = {
    val parser = new JawnParser
    Flow.fromFunction[ByteString, Json] { bytes =>
      parser.parseByteBuffer(bytes.asByteBuffer).fold(throw _, identity)
    }
  }

  /**
    * Send an HTTP request to the Clio server, optionally adding auth credentials.
    *
    * Retries the request on connection failures. Times out the request if
    * the first byte of the response fails to arrive within a timeout, or if a pull
    * on the open response stream fails to produce a next element within the same
    * timeout.
    *
    * For responses with a successful status code, return the entity bytes of the
    * response as a stream. For responses with an error status code, return a failed
    * stream of a single element containing the plaintext content of the response body.
    */
  private[clio] def dispatchRequest(
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
      .initialTimeout(responseTimeout)
      .idleTimeout(responseTimeout)

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
        response.entity.withoutSizeLimit().dataBytes.idleTimeout(responseTimeout)
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

  /** Get the version reported by the Clio server. */
  def getClioServerVersion: Source[Json, NotUsed] =
    dispatchRequest(HttpRequest(uri = s"/$versionString"), includeAuth = false)
      .via(parserFlow)

  /** Get the health reported by the Clio server. */
  def getClioServerHealth: Source[Json, NotUsed] =
    dispatchRequest(HttpRequest(uri = s"/$healthString"), includeAuth = false)
      .via(parserFlow)

  /**
    * Send new metadata to the Clio server for a document key.
    *
    * By default, the server will refuse to let the new metadata overwrite any
    * existing fields. Set `force` to `true` to disable this behavior.
    */
  def upsert[K <: IndexKey, M](clioIndex: ClioWebClient.UpsertAux[K, M])(
    key: K,
    metadata: M,
    force: Boolean = false
  ): Source[Json, NotUsed] = {
    import clioIndex.implicits._
    upsertJson(clioIndex)(key, metadata.asJson, force)
  }

  /**
    * Send arbitrary JSON to the Clio server as metadata for a document key.
    *
    * The server will validate that the JSON parses to valid metadata for the
    * key's type. By default, the server will refuse to let the new metadata
    * overwrite any existing fields. Set `force` to `true` to disable this behavior.
    */
  def upsertJson[K <: IndexKey](clioIndex: ClioWebClient.UpsertAux[K, _])(
    key: K,
    metadata: Json,
    force: Boolean = false
  ): Source[Json, NotUsed] = {
    val entity = HttpEntity(
      ContentTypes.`application/json`,
      metadata.pretty(ModelAutoDerivation.defaultPrinter)
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

  /**
    * Query an index backed by the Clio server using Clio's hand-rolled "simple" query DSL.
    *
    * For the most part, "simple" queries are transformed into ES queries by
    * taking each key-value pair and rewriting it into a "must" clause matching
    * documents with exactly the given value for that key. Minimal support exists
    * for querying ranges on date fields.
    */
  def simpleQuery[I](clioIndex: ClioWebClient.QueryAux[I])(
    input: I,
    includeDeleted: Boolean,
    includeAll: Boolean
  ): Source[Json, NotUsed] = {
    import clioIndex.implicits._
    if (includeDeleted || includeAll) {
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

  /**
    * Query the Clio server for metadata associated with a document key.
    *
    * Returns either an empty stream or a stream of a single element,
    * depending on whether or not any metadata has been posted into Clio
    * for the key.
    */
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
    val keyFields = FieldMapper[K].keys
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
        _.fold(Source.failed, _.fold(Source.empty[M])(Source.single))
      }
  }

  /**
    * Query an index backed by the Clio server.
    *
    * The query could be a JSON-ified "simple" query, or it could
    * be an arbitrary "raw" query using Elasticsearch's syntax.
    */
  def query(clioIndex: ClioIndex)(
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
