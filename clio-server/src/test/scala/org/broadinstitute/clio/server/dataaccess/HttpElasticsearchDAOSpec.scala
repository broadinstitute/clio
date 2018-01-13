package org.broadinstitute.clio.server.dataaccess

import java.util.UUID

import com.sksamuel.elastic4s.RefreshPolicy
import com.sksamuel.elastic4s.analyzers._
import com.sksamuel.elastic4s.bulk.BulkDefinition
import com.sksamuel.elastic4s.cluster.ClusterHealthDefinition
import com.sksamuel.elastic4s.delete.DeleteByIdDefinition
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.indexes.CreateIndexDefinition
import com.sksamuel.elastic4s.searches.SearchDefinition
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchUtil.RequestException
import org.broadinstitute.clio.server.dataaccess.elasticsearch._
import org.broadinstitute.clio.util.model.UpsertId
import org.scalatest._

import scala.concurrent.Future

class HttpElasticsearchDAOSpec
    extends AbstractElasticsearchDAOSpec("HttpElasticsearchDAO")
    with AsyncFlatSpecLike
    with Matchers
    with EitherValues
    with Elastic4sAutoDerivation {
  import com.sksamuel.elastic4s.circe._

  behavior of "HttpElasticsearch"

  it should "initialize" in {
    initialize()
  }

  it should "create an index and update the index field types" in {
    case class IndexVersion1(bar: String)
    case class IndexVersion2(foo: Long, bar: String)

    val indexVersion1: ElasticsearchIndex[_] =
      new AutoElasticsearchIndex[IndexVersion1]("test_index_update_type", 1)
    val indexVersion2: ElasticsearchIndex[_] =
      new AutoElasticsearchIndex[IndexVersion2]("test_index_update_type", 1)

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
      new AutoElasticsearchIndex[IndexVersion1]("test_index_fail_recreate", 1)
    val indexVersion2: ElasticsearchIndex[_] =
      new AutoElasticsearchIndex[IndexVersion2]("test_index_fail_recreate", 1)

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
    case class IndexVersion1(foo: String)
    case class IndexVersion2(foo: Long)

    val indexVersion1: ElasticsearchIndex[_] =
      new AutoElasticsearchIndex[IndexVersion1](
        "test_index_fail_change_types",
        1
      )
    val indexVersion2: ElasticsearchIndex[_] =
      new AutoElasticsearchIndex[IndexVersion2](
        "test_index_fail_change_types",
        1
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
          "mapper [foo] of different type, current_type [keyword], merged_type [long]"
        )
      }
    } yield succeed
  }

  it should "return the most recent document" in {
    import HttpElasticsearchDAOSpec._
    import org.broadinstitute.clio.server.dataaccess.elasticsearch._

    val index =
      new AutoElasticsearchIndex[Document]("docs-" + UUID.randomUUID(), 1)

    val documents =
      (1 to 4)
        .map("document-" + _)
        .map(s => Document(UpsertId.nextId(), Symbol(s)))

    for {
      _ <- httpElasticsearchDAO.createOrUpdateIndex(index)
      _ <- Future.sequence(
        documents.map(httpElasticsearchDAO.updateMetadata(_, index))
      )
      document <- httpElasticsearchDAO.getMostRecentDocument(index)
    } yield document should be(documents.lastOption)
  }

  it should "not throw an exception if no documents exist" in {
    import HttpElasticsearchDAOSpec._
    import org.broadinstitute.clio.server.dataaccess.elasticsearch._

    val index =
      new AutoElasticsearchIndex[Document]("docs-" + UUID.randomUUID(), 1)

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
      searchWithType("places" / "cities") scroll "1m" size 10 query {
        boolQuery must (
          queryStringQuery(""""London"""").defaultField("name"),
          queryStringQuery(""""Europe"""").defaultField("continent")
        )
      }

    val deleteDefinition: DeleteByIdDefinition =
      delete("uk") from "places" / "cities" refresh RefreshPolicy.WAIT_UNTIL

    lazy val httpClient = httpElasticsearchDAO.httpClient

    for {
      response <- httpClient.execute(clusterHealthDefinition)
      health = ElasticsearchUtil.unpackResponse(response)
      _ = health.status should be("green")
      response <- httpClient.execute(indexCreationDefinition)
      indexCreation = ElasticsearchUtil.unpackResponse(response)
      _ = indexCreation.acknowledged should be(true)
      response <- httpClient.execute(populateDefinition)
      populate = ElasticsearchUtil.unpackResponse(response)
      _ = populate.errors should be(false)
      response <- httpClient.execute(searchDefinition)
      search = ElasticsearchUtil.unpackResponse(response)
      _ = {
        search.hits.total should be(1)
        // Example using circe HitReader
        val city = search.to[City].head
        city.name should be("London")
        city.country should be("United Kingdom")
        city.continent should be("Europe")
        city.status should be("Awesome")
      }
      response <- httpClient.execute(deleteDefinition)
      delete = ElasticsearchUtil.unpackResponse(response)
      _ = delete.result should be("deleted")
      response <- httpClient.execute(deleteDefinition)
      delete = ElasticsearchUtil.unpackResponse(response)
      _ = delete.result shouldNot be("deleted")
    } yield succeed
  }
}

object HttpElasticsearchDAOSpec {
  case class Document(upsertId: UpsertId, entityId: Symbol) extends ClioDocument
}
