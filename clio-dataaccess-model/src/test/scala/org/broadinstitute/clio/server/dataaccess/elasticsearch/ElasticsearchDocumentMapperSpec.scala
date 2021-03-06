package org.broadinstitute.clio.server.dataaccess.elasticsearch

import org.broadinstitute.clio.transfer.model.{ModelMockKey, ModelMockMetadata}
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.UpsertId
import org.scalatest.{FlatSpec, Matchers}

class ElasticsearchDocumentMapperSpec
    extends FlatSpec
    with Matchers
    with ModelAutoDerivation {
  import org.broadinstitute.clio.JsonUtils.JsonOps

  behavior of "ElasticsearchDocumentMapper"

  val expectedId: UpsertId = UpsertId.nextId()

  val mapper = new ElasticsearchDocumentMapper[
    ModelMockKey,
    ModelMockMetadata
  ](() => expectedId)

  it should "create a document" in {
    val keyLong = 12345L
    val keyString = "key"

    val mockFieldDouble = 1.23
    val mockFieldInt = 456

    val key = ModelMockKey(keyLong, keyString)
    val metadata = ModelMockMetadata(
      mockFieldDouble = Option(mockFieldDouble),
      mockFieldInt = Option(mockFieldInt),
      mockFieldDate = None
    )

    val document = mapper.document(key, metadata)
    ElasticsearchIndex.getUpsertId(document) should be(expectedId)
    document.unsafeGet[Double]("mock_field_double") should be(mockFieldDouble)
    document.unsafeGet[Int]("mock_field_int") should be(mockFieldInt)
    document.unsafeGet[Long]("mock_key_long") should be(keyLong)
    document.unsafeGet[String]("mock_key_string") should be(keyString)
  }
}
