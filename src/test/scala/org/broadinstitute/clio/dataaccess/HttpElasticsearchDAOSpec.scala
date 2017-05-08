package org.broadinstitute.clio.dataaccess

import com.sksamuel.elastic4s.analyzers._
import com.sksamuel.elastic4s.bulk.BulkDefinition
import com.sksamuel.elastic4s.cluster.ClusterHealthDefinition
import com.sksamuel.elastic4s.delete.DeleteByIdDefinition
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.indexes.CreateIndexDefinition
import com.sksamuel.elastic4s.searches.SearchDefinition
import org.apache.http.HttpHost
import org.broadinstitute.clio.model.ElasticsearchStatusInfo
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy
import org.scalatest.{Assertion, AsyncFlatSpec, BeforeAndAfterAll, Matchers}

import scala.concurrent.Future
import scala.concurrent.duration._

class HttpElasticsearchDAOSpec extends AsyncFlatSpec with Matchers with BeforeAndAfterAll with ElasticsearchContainer {
  behavior of "HttpElasticsearch"

  lazy val httpHost = new HttpHost(elasticsearchContainerIpAddress, elasticsearchPort)
  lazy val httpElasticsearchDAO = new HttpElasticsearchDAO(Seq(httpHost))

  private val StatusGreen = "green"

  override protected def afterAll(): Unit = {
    // Not using regular .close within afterAll. The execution context provided by scalatest doesn't seem to run here.
    httpElasticsearchDAO.closeClient()
    super.afterAll()
  }

  it should "wait up to 60s for green status" in {
    def waitForGreen(count: Int, sleep: Duration): Future[Assertion] = {
      httpElasticsearchDAO.getClusterStatus flatMap {
        case ElasticsearchStatusInfo(StatusGreen, 1, 1) => Future.successful(succeed)
        case other if count == 0 => Future.failed(new RuntimeException(s"Timeout with status: $other"))
        case _ =>
          Thread.sleep(sleep.toMillis)
          waitForGreen(count - 1, sleep)
      }
    }

    waitForGreen(20, 3.seconds)
  }

  // TODO: The following tests are demo code, and should be replaced with actual DAO methods/specs.

  case class City(name: String, country: String, continent: String, status: String)

  it should "perform various CRUD-like operations" in {
    val clusterHealthDefinition: ClusterHealthDefinition =
      clusterHealth()

    val indexCreationDefinition: CreateIndexDefinition =
      createIndex("places") mappings {
        mapping("cities") as(
          keywordField("id"),
          textField("name") boost 4,
          textField("content") analyzer StopAnalyzer
        )
      }

    // Enabled circe Indexable and HitReader usage below
    import com.sksamuel.elastic4s.circe._
    import io.circe.generic.auto._

    val populateDefinition: BulkDefinition =
      bulk(
        /* Option 1: Fields syntax */
        indexInto("places" / "cities") id "uk" fields(
          "name" -> "London",
          "country" -> "United Kingdom",
          "continent" -> "Europe",
          "status" -> "Awesome"
        ),
        /* Option 2: Doc syntax */
        indexInto("places" / "cities") id "fr" doc
          City(
            name = "Paris",
            country = "France",
            continent = "Europe",
            status = "Awesome"
        )
      ).refresh(RefreshPolicy.WAIT_UNTIL)

    val searchDefinition: SearchDefinition =
      search("places" / "cities") scroll "1m" size 10 query {
        boolQuery must(
          queryStringQuery(""""London"""").defaultField("name"),
          queryStringQuery(""""Europe"""").defaultField("continent")
        )
      }

    val deleteDefinition: DeleteByIdDefinition =
      delete("uk") from "places" / "cities" refresh RefreshPolicy.WAIT_UNTIL

    lazy val httpClient = httpElasticsearchDAO.httpClient

    for {
      health <- httpClient execute clusterHealthDefinition
      _ = health.status should be(StatusGreen)
      indexCreation <- httpClient execute indexCreationDefinition
      _ = indexCreation.acknowledged should be(true)
      populate <- httpClient execute populateDefinition
      _ = populate.errors should be(false)
      search <- httpClient execute searchDefinition
      _ = {
        search.hits.total should be(1)
        // Example using circe HitReader
        val city = search.to[City].head
        city.name should be("London")
        city.country should be("United Kingdom")
        city.continent should be("Europe")
        city.status should be("Awesome")
      }
      delete <- httpClient execute deleteDefinition
      _ = delete.found should be(true)
      delete <- httpClient execute deleteDefinition
      _ = delete.found should be(false)
    } yield succeed
  }
}
