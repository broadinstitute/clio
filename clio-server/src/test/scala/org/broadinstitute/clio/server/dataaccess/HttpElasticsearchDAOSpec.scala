package org.broadinstitute.clio.server.dataaccess

import com.sksamuel.elastic4s.analyzers._
import com.sksamuel.elastic4s.bulk.BulkDefinition
import com.sksamuel.elastic4s.cluster.ClusterHealthDefinition
import com.sksamuel.elastic4s.delete.DeleteByIdDefinition
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.indexes.CreateIndexDefinition
import com.sksamuel.elastic4s.searches.SearchDefinition
import io.circe.parser
import org.apache.http.util.EntityUtils
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  AutoElasticsearchIndex,
  ElasticsearchIndex
}
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy
import org.elasticsearch.client.ResponseException
import org.scalatest._

class HttpElasticsearchDAOSpec
    extends AbstractElasticsearchDAOSpec("HttpElasticsearchDAO")
    with AsyncFlatSpecLike
    with Matchers
    with EitherValues {
  behavior of "HttpElasticsearch"

  it should "initialize" in {
    initialize()
  }

  it should "create an index and update the index field types" in {
    case class IndexVersion1(bar: String)
    case class IndexVersion2(foo: Long, bar: String)

    val indexVersion1: ElasticsearchIndex[_] =
      new AutoElasticsearchIndex[IndexVersion1]("test_index_update_type")
    val indexVersion2: ElasticsearchIndex[_] =
      new AutoElasticsearchIndex[IndexVersion2]("test_index_update_type")

    for {
      _ <- httpElasticsearchDAO.createIndexType(indexVersion1)
      existsVersion1 <- httpElasticsearchDAO.existsIndexType(indexVersion1)
      _ = existsVersion1 should be(true)
      existsVersion2 <- httpElasticsearchDAO.existsIndexType(indexVersion2)
      _ = existsVersion2 should be(true)
      _ <- httpElasticsearchDAO.updateFieldDefinitions(indexVersion1)
      _ <- httpElasticsearchDAO.updateFieldDefinitions(indexVersion2)
    } yield succeed
  }

  it should "fail to recreate an index twice when skipping check for existence" in {
    case class IndexVersion1(foo: String)
    case class IndexVersion2(foo: Long, bar: String)

    val indexVersion1: ElasticsearchIndex[_] =
      new AutoElasticsearchIndex[IndexVersion1]("test_index_fail_recreate")
    val indexVersion2: ElasticsearchIndex[_] =
      new AutoElasticsearchIndex[IndexVersion2]("test_index_fail_recreate")

    for {
      _ <- httpElasticsearchDAO.createIndexType(indexVersion1)
      exception <- recoverToExceptionIf[ResponseException] {
        httpElasticsearchDAO.createIndexType(indexVersion2)
      }
      _ = {
        val json = EntityUtils.toString(exception.getResponse.getEntity)
        val doc = parser.parse(json).right.value
        val cursor = doc.hcursor
        val error = cursor.downField("error")
        val errorType = error.get[String]("type").right.value
        val errorReason = error.get[String]("reason").right.value
        errorType should be("index_already_exists_exception")
        errorReason should fullyMatch regex """index \[test_index_fail_recreate/.*\] already exists"""
      }
    } yield succeed
  }

  it should "fail to change the index field types" in {
    case class IndexVersion1(foo: String)
    case class IndexVersion2(foo: Long)

    val indexVersion1: ElasticsearchIndex[_] =
      new AutoElasticsearchIndex[IndexVersion1]("test_index_fail_change_types")
    val indexVersion2: ElasticsearchIndex[_] =
      new AutoElasticsearchIndex[IndexVersion2]("test_index_fail_change_types")
    for {
      _ <- httpElasticsearchDAO.createIndexType(indexVersion1)
      _ <- httpElasticsearchDAO.updateFieldDefinitions(indexVersion1)
      exception <- recoverToExceptionIf[ResponseException] {
        httpElasticsearchDAO.updateFieldDefinitions(indexVersion2)
      }
      _ = {
        val json = EntityUtils.toString(exception.getResponse.getEntity)
        val doc = parser.parse(json).right.value
        val cursor = doc.hcursor
        val error = cursor.downField("error")
        val errorType = error.get[String]("type").right.value
        val errorReason = error.get[String]("reason").right.value
        errorType should be("illegal_argument_exception")
        errorReason should be(
          "mapper [foo] of different type, current_type [keyword], merged_type [long]"
        )
      }
    } yield succeed
  }

  // TODO: The following tests are demo code, and should be replaced with actual DAO methods/specs.

  case class City(name: String,
                  country: String,
                  continent: String,
                  status: String,
                  slogan: Option[String])

  it should "perform various CRUD-like operations" in {
    val clusterHealthDefinition: ClusterHealthDefinition =
      clusterHealth()

    val indexCreationDefinition: CreateIndexDefinition =
      createIndex("places") replicas 0 mappings {
        mapping("cities") as (
          keywordField("id"),
          textField("name") boost 4,
          textField("content") analyzer StopAnalyzer
        )
      }

    // Enabled circe Indexable and HitReader usage below
    import com.sksamuel.elastic4s.circe._
    import org.broadinstitute.clio.server.dataaccess.elasticsearch.Elastic4sAutoDerivation._

    val populateDefinition: BulkDefinition =
      bulk(
        /* Option 1: Fields syntax */
        indexInto("places" / "cities") id "uk" fields (
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
            status = "Awesome",
            slogan = Option("Liberté, égalité, fraternité")
          ),
        indexInto("places" / "cities") id "de" doc
          City(
            name = "Berlin",
            country = "Germany",
            continent = "Europe",
            status = "Awesome",
            slogan = None
          )
      ).refresh(RefreshPolicy.WAIT_UNTIL)

    val searchDefinition: SearchDefinition =
      search("places" / "cities") scroll "1m" size 10 query {
        boolQuery must (
          queryStringQuery(""""London"""").defaultField("name"),
          queryStringQuery(""""Europe"""").defaultField("continent")
        )
      }

    val deleteDefinition: DeleteByIdDefinition =
      delete("uk") from "places" / "cities" refresh RefreshPolicy.WAIT_UNTIL

    lazy val httpClient = httpElasticsearchDAO.httpClient

    for {
      health <- httpClient execute clusterHealthDefinition
      _ = health.status should be("green")
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
