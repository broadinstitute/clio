package org.broadinstitute.clio.server.dataaccess.elasticsearch

import java.net.URI
import java.time.OffsetDateTime

import com.sksamuel.elastic4s.http.ElasticDsl._
import io.circe.syntax._
import org.broadinstitute.clio.transfer.model.{
  ModelMockIndex,
  ModelMockKey,
  ModelMockMetadata,
  ModelMockQueryInput
}
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.{DocumentStatus, UpsertId}
import org.scalatest.{FlatSpec, Matchers}

class ElasticsearchQueryMapperSpec
    extends FlatSpec
    with Matchers
    with ModelAutoDerivation {
  behavior of "ElasticsearchQueryMapper"

  it should "return isEmpty true for an empty input" in {
    val mapper = ElasticsearchQueryMapper[ModelMockQueryInput]
    val input = ModelMockQueryInput(
      mockFieldDateEnd = None,
      mockFieldDateStart = None,
      mockFieldDouble = None,
      mockFieldInt = None,
      mockKeyLong = None,
      mockKeyString = None
    )
    mapper.isEmpty(input) should be(true)
  }

  it should "return isEmpty false for a non-empty input" in {
    val mapper = ElasticsearchQueryMapper[ModelMockQueryInput]
    val input = ModelMockQueryInput(
      mockFieldDateEnd = None,
      mockFieldDateStart = None,
      mockFieldDouble = None,
      mockFieldInt = None,
      mockKeyLong = None,
      mockKeyString = Option("hello")
    )
    mapper.isEmpty(input) should be(false)
  }

  it should "successfully buildQuery" in {
    val mapper = ElasticsearchQueryMapper[ModelMockQueryInput]
    val endDate = OffsetDateTime.parse("1972-01-01T12:34:56.789+05:00")
    val startDate = OffsetDateTime.parse("1971-01-01T12:34:56.789+05:00")
    val input = ModelMockQueryInput(
      mockFieldDateEnd = Option(endDate),
      mockFieldDateStart = Option(startDate),
      mockFieldDouble = None,
      mockFieldInt = None,
      mockKeyLong = None,
      mockKeyString = Option("hello")
    )
    val index = new ElasticsearchIndex(
      ModelMockIndex(),
      Map.empty[String, String].asJson,
      ElasticsearchFieldMapper.StringsToTextFieldsWithSubKeywords
    )
    mapper.buildQuery(input)(index) should be(
      boolQuery must (
        rangeQuery("mock_field_date").lte(endDate.toString),
        rangeQuery("mock_field_date").gte(startDate.toString),
        queryStringQuery(""""hello"""").defaultField(
          s"mock_key_string.${ElasticsearchFieldMapper.StringsToTextFieldsWithSubKeywords.TextExactMatchFieldName}"
        )
      )
    )
  }

  it should "strip internal fields in toQueryOutput" in {
    val mapper = ElasticsearchQueryMapper[ModelMockQueryInput]
    val keyLong = 1L
    val keyString = "mock-key-1"
    val key = ModelMockKey(keyLong, keyString)
    val metadata = ModelMockMetadata(
      Some(1.234),
      Some(1234),
      Some(OffsetDateTime.now()),
      Some(Seq.empty[String]),
      Some(Seq.empty[URI]),
      Some(DocumentStatus.Normal),
      Some('md5),
      Some(URI.create("gs://the-file")),
      Some(1234L)
    )
    val fields = key.asJson.deepMerge(metadata.asJson)
    val output = fields
      .deepMerge(
        Map(ElasticsearchIndex.UpsertIdElasticsearchName -> UpsertId.nextId()).asJson
      )
      .deepMerge(
        Map(ElasticsearchIndex.EntityIdElasticsearchName -> s"$keyLong.$keyString").asJson
      )
    mapper.toQueryOutput(output) should be(fields)
  }
}
