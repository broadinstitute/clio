package org.broadinstitute.clio.server.dataaccess

import java.util.UUID

import com.sksamuel.elastic4s.RefreshPolicy
import com.sksamuel.elastic4s.analyzers._
import com.sksamuel.elastic4s.bulk.BulkRequest
import com.sksamuel.elastic4s.cluster.ClusterHealthRequest
import com.sksamuel.elastic4s.delete.DeleteByIdRequest
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.indexes.CreateIndexRequest
import com.sksamuel.elastic4s.searches.SearchRequest
import io.circe.Json
import io.circe.syntax._
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchUtil.RequestException
import org.broadinstitute.clio.server.dataaccess.elasticsearch._
import org.broadinstitute.clio.transfer.model.{HealthStatus, ModelMockIndex, ModelMockKey}
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.UpsertId
import org.scalatest._

import scala.concurrent.Future

class HttpElasticsearchDAOSpec
    extends AbstractElasticsearchDAOSpec("HttpElasticsearchDAO")
    with AsyncFlatSpecLike
    with Matchers
    with EitherValues
    with ModelAutoDerivation
    with OptionValues {
  import ElasticsearchUtil.HttpClientOps
  import com.sksamuel.elastic4s.circe._

  behavior of "HttpElasticsearch"

  it should "create an index and update the index field types" in {
    val indexVersion1: ElasticsearchIndex[_] =
      new ElasticsearchIndex[ModelMockIndex](
        ModelMockIndex(),
        "test_index_update_type",
        ElasticsearchFieldMapper.NumericBooleanDateAndKeywordFields
      )
    val indexVersion2: ElasticsearchIndex[_] =
      new ElasticsearchIndex[ModelMockIndex](
        ModelMockIndex(commandName = "command_name"),
        "test_index_update_type",
        ElasticsearchFieldMapper.NumericBooleanDateAndKeywordFields
      )

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
    val indexVersion1: ElasticsearchIndex[_] =
      new ElasticsearchIndex[ModelMockIndex](
        ModelMockIndex(),
        "test_index_fail_recreate",
        ElasticsearchFieldMapper.NumericBooleanDateAndKeywordFields
      )
    val indexVersion2: ElasticsearchIndex[_] =
      new ElasticsearchIndex[ModelMockIndex](
        ModelMockIndex(commandName = "command_name"),
        "test_index_fail_recreate",
        ElasticsearchFieldMapper.NumericBooleanDateAndKeywordFields
      )

    for {
      _ <- httpElasticsearchDAO.createIndexType(indexVersion1)
      exception <- recoverToExceptionIf[RequestException] {
        httpElasticsearchDAO.createIndexType(indexVersion2)
      }
      _ = {
        val error = exception.requestFailure.error
        error.`type` should be("index_already_exists_exception")
        error.reason should fullyMatch regex """index \[test_index_fail_recreate/.*\] already exists"""
      }
    } yield succeed
  }

  it should "fail to change the index field types" in {
    val indexVersion1: ElasticsearchIndex[_] =
      new ElasticsearchIndex[ModelMockIndex](
        ModelMockIndex(),
        "test_index_fail_change_types",
        ElasticsearchFieldMapper.NumericBooleanDateAndKeywordFields
      )
    val indexVersion2: ElasticsearchIndex[_] =
      new ElasticsearchIndex[ModelMockIndex](
        ModelMockIndex(),
        "test_index_fail_change_types",
        ElasticsearchFieldMapper.StringsToTextFieldsWithSubKeywords
      )
    for {
      _ <- httpElasticsearchDAO.createIndexType(indexVersion1)
      _ <- httpElasticsearchDAO.updateFieldDefinitions(indexVersion1)
      exception <- recoverToExceptionIf[RequestException] {
        httpElasticsearchDAO.updateFieldDefinitions(indexVersion2)
      }
      _ = {
        val error = exception.requestFailure.error
        error.`type` should be("illegal_argument_exception")
        error.reason should be(
          "mapper [mock_string_array] of different type, current_type [keyword], merged_type [text]"
        )
      }
    } yield succeed
  }

  it should "return the most recent document" in {
    import org.broadinstitute.clio.server.dataaccess.elasticsearch._

    val keyLong = 12345L
    val keyString = "key"
    val index = new ElasticsearchIndex[ModelMockIndex](
      ModelMockIndex(),
      "docs-" + UUID.randomUUID(),
      ElasticsearchFieldMapper.NumericBooleanDateAndKeywordFields
    )

    def generateBookkeeping(s: String): Json =
      Map(
        ElasticsearchIndex.UpsertIdElasticsearchName -> UpsertId.nextId()
      ).asJson
        .deepMerge(ModelMockKey(keyLong, keyString + s).asJson)

    val documents =
      (1 to 4)
        .map("document-" + _)
        .map(generateBookkeeping)

    for {
      _ <- httpElasticsearchDAO.createOrUpdateIndex(index)
      _ <- Future.sequence(
        documents.map(httpElasticsearchDAO.updateMetadata(_)(index))
      )
      document <- httpElasticsearchDAO.getMostRecentDocument(index)
    } yield document.value should be(documents.last)
  }

  it should "not throw an exception if no documents exist" in {
    import org.broadinstitute.clio.server.dataaccess.elasticsearch._

    val index = new ElasticsearchIndex[ModelMockIndex](
      ModelMockIndex(),
      "docs-" + UUID.randomUUID(),
      ElasticsearchFieldMapper.NumericBooleanDateAndKeywordFields
    )

    for {
      _ <- httpElasticsearchDAO.createOrUpdateIndex(index)
      res <- httpElasticsearchDAO.getMostRecentDocument(index)
    } yield {
      res should be(None)
    }
  }

  case class City(
    name: String,
    country: String,
    continent: String,
    status: String,
    slogan: Option[String]
  )

  it should "perform various CRUD-like operations" in {
    val clusterHealthDefinition: ClusterHealthRequest =
      clusterHealth()

    val indexCreationDefinition: CreateIndexRequest =
      createIndex("places") replicas 0 mappings {
        mapping("cities") as (
          keywordField("id"),
          textField("name") boost 4,
          textField("content") analyzer StopAnalyzer
        )
      }

    val populateDefinition: BulkRequest =
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

    val searchDefinition: SearchRequest =
      searchWithType("places" / "cities") scroll "1m" size 10 query {
        boolQuery must (
          queryStringQuery(""""London"""").defaultField("name"),
          queryStringQuery(""""Europe"""").defaultField("continent")
        )
      }

    val deleteDefinition: DeleteByIdRequest =
      delete("uk") from "places" / "cities" refresh RefreshPolicy.WAIT_UNTIL

    lazy val httpClient = httpElasticsearchDAO.elasticClient

    for {
      health <- httpClient.executeAndUnpack(clusterHealthDefinition)
      _ = HealthStatus.withNameInsensitive(health.status) should be(HealthStatus.Green)
      indexCreation <- httpClient.executeAndUnpack(indexCreationDefinition)
      _ = indexCreation.acknowledged should be(true)
      populate <- httpClient.executeAndUnpack(populateDefinition)
      _ = populate.errors should be(false)
      search <- httpClient.executeAndUnpack(searchDefinition)
      _ = {
        search.hits.total should be(1)
        // Example using circe HitReader
        val city = search.to[City].head
        city.name should be("London")
        city.country should be("United Kingdom")
        city.continent should be("Europe")
        city.status should be("Awesome")
      }
      delete1 <- httpClient.executeAndUnpack(deleteDefinition)
      delete2 <- httpClient.executeAndUnpack(deleteDefinition)
    } yield {
      delete1.result should be("deleted")
      delete2.result shouldNot be("deleted")
    }
  }
}
