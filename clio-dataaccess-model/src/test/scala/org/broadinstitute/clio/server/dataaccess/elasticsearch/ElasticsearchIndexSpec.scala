package org.broadinstitute.clio.server.dataaccess.elasticsearch

import com.sksamuel.elastic4s.http.ElasticDsl._
import org.scalatest.{FlatSpec, Matchers}

class ElasticsearchIndexSpec extends FlatSpec with Matchers with Elastic4sAutoDerivation {
  behavior of "ElasticsearchIndex"

  import com.sksamuel.elastic4s.circe._

  it should behave like aV1Index(
    new ElasticsearchIndex[DocumentMock](
      "mock",
      ElasticsearchFieldMapper.NumericBooleanDateAndKeywordFields
    )
  )
  it should behave like aV2Index(
    new ElasticsearchIndex[DocumentMock](
      "mock-v2",
      ElasticsearchFieldMapper.StringsToTextFieldsWithSubKeywords
    )
  )

  def aV1Index(index: ElasticsearchIndex[DocumentMock]): Unit = {
    it should "indexName for v1 document" in {
      index.indexName should be("mock")
    }

    it should "indexType for v1 document" in {
      index.indexType should be("default")
    }

    it should "fields for v1 document" in {
      // Snake-case-ify the bookkeeping fields.
      val bookkeeping =
        Seq(ClioDocument.UpsertIdFieldName, ClioDocument.EntityIdFieldName).map { name =>
          keywordField(name.replaceAll("([A-Z])", "_$1").toLowerCase)
        }

      index.fields should contain theSameElementsAs bookkeeping ++ Seq(
        dateField("mock_field_date"),
        doubleField("mock_field_double"),
        intField("mock_field_int"),
        keywordField("mock_file_md5"),
        keywordField("mock_file_path"),
        longField("mock_file_size"),
        longField("mock_key_long"),
        keywordField("mock_key_string"),
        keywordField("mock_string_array"),
        keywordField("mock_path_array")
      )
    }
  }

  def aV2Index(index: ElasticsearchIndex[DocumentMock]): Unit = {
    it should "indexName for v2 document" in {
      index.indexName should be("mock-v2")
    }

    it should "indexType for v2 document" in {
      index.indexType should be("default")
    }

    it should "fields for v2 document" in {
      // Snake-case-ify the bookkeeping fields.
      val bookkeeping =
        Seq(ClioDocument.UpsertIdFieldName, ClioDocument.EntityIdFieldName).map { name =>
          keywordField(ElasticsearchUtil.toElasticsearchName(name))
        }

      import ElasticsearchFieldMapper.StringsToTextFieldsWithSubKeywords.TextExactMatchFieldName

      index.fields should contain theSameElementsAs bookkeeping ++ Seq(
        dateField("mock_field_date"),
        doubleField("mock_field_double"),
        intField("mock_field_int"),
        keywordField("mock_file_md5"),
        keywordField("mock_file_path"),
        longField("mock_file_size"),
        longField("mock_key_long"),
        textField("mock_key_string").fields(keywordField(TextExactMatchFieldName)),
        textField("mock_string_array").fields(keywordField(TextExactMatchFieldName)),
        keywordField("mock_path_array")
      )
    }
  }

}
