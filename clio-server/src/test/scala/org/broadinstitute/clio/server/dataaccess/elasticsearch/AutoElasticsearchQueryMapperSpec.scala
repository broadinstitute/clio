package org.broadinstitute.clio.server.dataaccess.elasticsearch

import java.time.OffsetDateTime

import com.sksamuel.elastic4s.http.ElasticDsl._
import org.broadinstitute.clio.server.model.{ModelMockQueryInput, ModelMockQueryOutput}
import org.scalatest.{FlatSpec, Matchers}

class AutoElasticsearchQueryMapperSpec extends FlatSpec with Matchers {
  behavior of "AutoElasticsearchQueryMapper"

  it should "return isEmpty true for an empty input" in {
    val mapper = AutoElasticsearchQueryMapper[
      ModelMockQueryInput,
      ModelMockQueryOutput,
      DocumentMock
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
    val mapper = AutoElasticsearchQueryMapper[
      ModelMockQueryInput,
      ModelMockQueryOutput,
      DocumentMock
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
    val mapper = AutoElasticsearchQueryMapper[
      ModelMockQueryInput,
      ModelMockQueryOutput,
      DocumentMock
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
    mapper.buildQuery(input)(DocumentMock.index) should be(
      boolQuery must (
        rangeQuery("mock_field_date").lte(endDate.toString),
        rangeQuery("mock_field_date").gte(startDate.toString),
        queryStringQuery(""""hello"""").defaultField("mock_key_string")
      )
    )
    mapper.buildQuery(input)(DocumentMock.indexWithTextFields) should be(
      boolQuery must (
        rangeQuery("mock_field_date").lte(endDate.toString),
        rangeQuery("mock_field_date").gte(startDate.toString),
        queryStringQuery(""""hello"""").defaultField(
          s"mock_key_string.${ElasticsearchFieldMapper.StringsToTextFieldsWithSubKeywords.TextExactMatchFieldName}"
        )
      )
    )
  }

  it should "toQueryOutput" in {
    val mapper = AutoElasticsearchQueryMapper[
      ModelMockQueryInput,
      ModelMockQueryOutput,
      DocumentMock
    ]
    val output = DocumentMock.default
    mapper.toQueryOutput(output) should be(
      ModelMockQueryOutput(
        mockFieldDate = output.mockFieldDate,
        mockFieldDouble = output.mockFieldDouble,
        mockFieldInt = output.mockFieldInt,
        mockFileMd5 = output.mockFileMd5,
        mockFilePath = output.mockFilePath,
        mockFileSize = output.mockFileSize,
        mockKeyLong = output.mockKeyLong,
        mockKeyString = output.mockKeyString
      )
    )
  }
}
