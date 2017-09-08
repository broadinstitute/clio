package org.broadinstitute.clio.integrationtest

import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.{ClioClient, ClioClientConfig}
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ClioDocument,
  ElasticsearchIndex
}
import org.broadinstitute.clio.util.json.ModelAutoDerivation

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.index.mappings.IndexMappings
import com.typesafe.scalalogging.LazyLogging
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport
import io.circe.{Decoder, Encoder, Printer}
import io.circe.parser._
import io.circe.syntax._
import org.apache.http.HttpHost
import org.elasticsearch.client.RestClient
import org.scalatest.{AsyncFlatSpecLike, BeforeAndAfterAll, Matchers}

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import java.nio.file.{Files, Path}
import java.util.UUID

/**
  * Base class for Clio integration tests, agnostic to the location
  * of the Clio / Elasticsearch instances that will be tested against.
  */
abstract class BaseIntegrationSpec(clioDescription: String)
    extends TestKit(ActorSystem(clioDescription.replaceAll("\\s+", "-")))
    with AsyncFlatSpecLike
    with BeforeAndAfterAll
    with Matchers
    with ModelAutoDerivation
    with ErrorAccumulatingCirceSupport
    with LazyLogging {

  behavior of clioDescription

  implicit val m: ActorMaterializer = ActorMaterializer()

  /**
    * Printer to override Circe's default marshalling behavior
    * for None from 'None -> null' to omitting Nones entirely.
    */
  implicit val p: Printer = Printer.noSpaces.copy(dropNullKeys = true)

  /**
    * Timeout to use for all client requests.
    *
    * Use the client's default to make sure it's sane.
    */
  val clientTimeout: FiniteDuration = ClioClientConfig.responseTimeout

  /** The bearer token to use when hitting the /api route of Clio. */
  implicit def bearerToken: OAuth2BearerToken

  /**
    * The web client to use within the tested clio-client.
    */
  def clioWebClient: ClioWebClient

  /**
    * The clio-client to test against.
    */
  lazy val clioClient: ClioClient = new ClioClient(clioWebClient, IoUtil)

  /**
    * The URI of the Elasticsearch instance to test in a suite.
    * Could point to a local Docker container, or to a deployed ES node.
    */
  def elasticsearchUri: Uri

  /**
    * Path to the root directory in which metadata updates will
    * be persisted.
    */
  def rootPersistenceDir: Path

  /**
    * HTTP client pointing to the elasticsearch node to test against.
    *
    * Needs to be lazy because otherwise it'll raise an
    * UninitializedFieldError in the containerized spec because of
    * the initialization-order weirdness around the container field.
    */
  lazy val elasticsearchClient: HttpClient = {
    val host = HttpHost.create(elasticsearchUri.toString())
    val restClient = RestClient.builder(host).build()
    HttpClient.fromRestClient(restClient)
  }

  /**
    * Run a command with arbitrary args through the main clio-client.
    *
    * Fails the test immediately if the client exits early.
    */
  def runClient(command: String, args: String*): Future[HttpResponse] = {
    clioClient
      .instanceMain(
        (Seq("--bearer-token", bearerToken.token, command) ++ args).toArray
      )
      .fold(err => fail(s"Command exited early with $err"), identity)
  }

  /**
    * Convert one of our [[org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex]]
    * instances into an [[com.sksamuel.elastic4s.http.index.mappings.IndexMappings]], for comparison.
    */
  def indexToMapping(index: ElasticsearchIndex[_]): IndexMappings = {
    val fields = index.fields
      .map { field =>
        field.name -> Map("type" -> field.`type`)
      }
      .toMap[String, Any]

    IndexMappings(index.indexName, Map(index.indexType -> fields))
  }

  /** Get a random identifier to use as a dummy value in testing. */
  def randomId: String = UUID.randomUUID().toString.replaceAll("-", "")

  /**
    * Check that the storage path for a given Clio ID exists,
    * load its contents as JSON, and check that the loaded ID
    * matches the given ID.
    */
  def getJsonFrom[Document <: ClioDocument: Decoder](
    index: ElasticsearchIndex[Document],
    clioId: UUID
  ): Document = {
    val expectedPath =
      rootPersistenceDir.resolve(s"${index.currentPersistenceDir}/$clioId.json")

    Files.exists(expectedPath) should be(true)
    val document = parse(new String(Files.readAllBytes(expectedPath)))
      .flatMap(_.as[Document])
      .toTry
      .get

    document.clioId should be(clioId)
    document
  }

  /**
    * Write an object as JSON to a temp file.
    *
    * Registers the temp file for deletion.
    */
  def writeTmpJson[A: Encoder](obj: A): Path = {
    val tmpFile = Files.createTempFile("clio-integration", ".json")
    val json = obj.asJson.pretty(implicitly[Printer])
    Files.write(tmpFile, json.getBytes)

    tmpFile.toFile.deleteOnExit()
    tmpFile
  }

  /** Shut down the actor system at the end of the suite. */
  override protected def afterAll(): Unit = {
    shutdown()
    super.afterAll()
  }
}
