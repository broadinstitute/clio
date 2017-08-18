package org.broadinstitute.clio.server.dataaccess.elasticsearch

import com.sksamuel.elastic4s.http.ElasticDsl._
import org.scalatest.{FlatSpec, Matchers}

class ElasticsearchIndexSpec extends FlatSpec with Matchers {

  behavior of "ElasticsearchIndex"

  it should "indexName for indexDocument" in {
    val index = ElasticsearchIndex.indexDocument[DocumentMock]
    index.indexName should be("mock")
  }

  it should "indexType for indexDocument" in {
    val index = ElasticsearchIndex.indexDocument[DocumentMock]
    index.indexType should be("default")
  }

  it should "fields for indexDocument" in {
    val index = ElasticsearchIndex.indexDocument[DocumentMock]
    index.fields should contain theSameElementsInOrderAs Seq(
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
