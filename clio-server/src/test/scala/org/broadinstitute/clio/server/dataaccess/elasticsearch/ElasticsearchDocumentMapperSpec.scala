package org.broadinstitute.clio.server.dataaccess.elasticsearch

import org.broadinstitute.clio.util.model.UpsertId
import org.broadinstitute.clio.transfer.model.{ModelMockKey, ModelMockMetadata}
import org.broadinstitute.clio.util.json.DecodingUtil
import org.scalatest.{FlatSpec, Matchers}

class ElasticsearchDocumentMapperSpec extends FlatSpec with Matchers with DecodingUtil {
  behavior of "AutoElasticsearchDocumentMapper"

  val expectedId: UpsertId = UpsertId.nextId()

  val mapper = new ElasticsearchDocumentMapper[
    ModelMockKey,
    ModelMockMetadata
  ](() => expectedId)

  it should "create a document" in {
    val keyLong = 12345L
    val keyString = "key"

    val mockFieldDouble = Option(1.23)
    val mockFieldInt = Option(456)

    val key = ModelMockKey(keyLong, keyString)
    val metadata = ModelMockMetadata(
      mockFieldDouble = mockFieldDouble,
      mockFieldInt = mockFieldInt,
      mockFieldDate = None
    )

    val document = mapper.document(key, metadata)
    ElasticsearchIndex.getUpsertId(document) should be(expectedId)
    ElasticsearchIndex.getEntityId(document) should be(s"$keyLong.$keyString")
    getDoubleByName(document, "mock_field_double") should be(mockFieldDouble.get)
    getIntByName(document, "mock_field_int") should be(mockFieldInt.get)
    getLongByName(document, "mock_key_long") should be(keyLong)
    getStringByName(document, "mock_key_string") should be(keyString)
  }
}
