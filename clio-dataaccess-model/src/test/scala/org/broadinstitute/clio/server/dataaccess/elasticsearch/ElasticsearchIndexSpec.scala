package org.broadinstitute.clio.server.dataaccess.elasticsearch

import com.sksamuel.elastic4s.http.ElasticDsl._
import org.scalatest.{FlatSpec, Matchers}

class ElasticsearchIndexSpec extends FlatSpec with Matchers {

  Map(
    "ElasticsearchIndex" -> ElasticsearchIndex.indexDocument[DocumentMock],
    "AutoElasticsearchIndex" -> new AutoElasticsearchIndex[DocumentMock]("mock")
  ).foreach {
    case (description, index) => {
      behavior of description
      it should behave like anIndex(index)
    }
  }

  def anIndex[D](index: ElasticsearchIndex[D]): Unit = {
    it should "indexName for indexDocument" in {
      index.indexName should be("mock")
    }

    it should "indexType for indexDocument" in {
      index.indexType should be("default")
    }

    it should "fields for indexDocument" in {
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
}
