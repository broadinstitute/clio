package org.broadinstitute.clio.server.dataaccess.elasticsearch

import com.sksamuel.elastic4s.http.ElasticDsl._
import org.scalatest.{FlatSpec, Matchers}

class AutoElasticsearchIndexSpec extends FlatSpec with Matchers {

  behavior of "AutoElasticsearchIndex"

  it should "indexName" in {
    val index = new AutoElasticsearchIndex[DocumentMock]("hello")
    index.indexName should be("hello")
  }

  it should "indexType" in {
    val index = new AutoElasticsearchIndex[DocumentMock]("hello")
    index.indexType should be("default")
  }

  it should "fields" in {
    val index = new AutoElasticsearchIndex[DocumentMock]("hello")
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
