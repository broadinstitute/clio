package org.broadinstitute.clio.server.dataaccess.elasticsearch

import com.sksamuel.elastic4s.http.ElasticDsl._
import org.scalatest.{FlatSpec, Matchers}

class ElasticsearchIndexSpec
    extends FlatSpec
    with Matchers
    with Elastic4sAutoDerivation {
  import com.sksamuel.elastic4s.circe._

  Map(
    "ElasticsearchIndex" ->
      (
        (version: Int) =>
          ElasticsearchIndex.indexDocument[DocumentMock](version)
      ),
    "AutoElasticsearchIndex" ->
      (
        (version: Int) =>
          new AutoElasticsearchIndex[DocumentMock]("mock", version)
      )
  ).foreach {
    case (description, index) => {
      behavior of description
      it should behave like aV1Index(index(1))
      it should behave like aV2Index(index(2))
    }
  }

  def aV1Index[D](index: ElasticsearchIndex[D]): Unit = {
    it should "indexName for v1 document" in {
      index.indexName should be("mock")
    }

    it should "indexType for v1 document" in {
      index.indexType should be("default")
    }

    it should "fields for v1 document" in {
      // Snake-case-ify the bookkeeping fields.
      val bookkeeping =
        Seq(ClioDocument.UpsertIdFieldName, ClioDocument.EntityIdFieldName)
          .map { name =>
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
        keywordField("mock_key_string")
      )
    }
  }

  def aV2Index[D](index: ElasticsearchIndex[D]): Unit = {
    it should "indexName for v2 document" in {
      index.indexName should be("mock_v2")
    }

    it should "indexType for v2 document" in {
      index.indexType should be("default")
    }

    it should "fields for v2 document" in {
      // Snake-case-ify the bookkeeping fields.
      val bookkeeping =
        Seq(ClioDocument.UpsertIdFieldName, ClioDocument.EntityIdFieldName)
          .map { name =>
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
        textField("mock_key_string").fields(keywordField("exact"))
      )
    }
  }

}
