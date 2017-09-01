package org.broadinstitute.clio.integrationtest

import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.index.mappings.IndexMappings
import com.typesafe.scalalogging.LazyLogging
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport
import io.circe.Printer
import org.apache.http.HttpHost
import org.elasticsearch.client.RestClient
import org.scalatest.{AsyncFlatSpecLike, BeforeAndAfterAll, Matchers}
import java.util.UUID

import akka.http.scaladsl.model.headers.OAuth2BearerToken

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
    * The web client to test against.
    */
  def clioWebClient: ClioWebClient

  /**
    * The URI of the Elasticsearch instance to test in a suite.
    * Could point to a local Docker container, or to a deployed ES node.
    */
  def elasticsearchUri: Uri

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

  /** The bearer token to use when hitting the /api route of Clio. */
  implicit val bearerToken: OAuth2BearerToken

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

  /** Shut down the actor system at the end of the suite. */
  override protected def afterAll(): Unit = {
    shutdown()
    super.afterAll()
  }
}
