package org.broadinstitute.clio.server.dataaccess.elasticsearch

import java.time.OffsetDateTime

import com.sksamuel.elastic4s.http.ElasticDsl._
import org.broadinstitute.clio.transfer.model.{ModelMockIndex, ModelMockQueryInput}
import org.scalatest.{FlatSpec, Matchers}

class ElasticsearchQueryMapperSpec extends FlatSpec with Matchers {
  behavior of "AutoElasticsearchQueryMapper"

  it should "return isEmpty true for an empty input" in {
    val mapper = ElasticsearchQueryMapper[
      ModelMockQueryInput
    ]
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
    val mapper = ElasticsearchQueryMapper[
      ModelMockQueryInput
    ]
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

  it should "buildQuery" in {
    val mapper = ElasticsearchQueryMapper[
      ModelMockQueryInput
    ]
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
      "mock",
      ModelMockIndex(),
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
}
