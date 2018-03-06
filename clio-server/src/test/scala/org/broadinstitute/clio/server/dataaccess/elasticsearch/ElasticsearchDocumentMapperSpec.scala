package org.broadinstitute.clio.server.dataaccess.elasticsearch

import org.broadinstitute.clio.util.model.UpsertId
import io.circe.Json
import io.circe.parser.parse
import org.broadinstitute.clio.transfer.model.{ModelMockKey, ModelMockMetadata}
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.scalatest.{FlatSpec, Matchers}

class ElasticsearchDocumentMapperSpec
    extends FlatSpec
    with Matchers
    with ModelAutoDerivation {
  behavior of "AutoElasticsearchDocumentMapper"

  val expectedId: UpsertId = UpsertId.nextId()

  val mapper = new ElasticsearchDocumentMapper[
    ModelMockKey,
    ModelMockMetadata
  ](() => expectedId)

  it should "document" in {
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
    val json =
      parse("""
        "mockFieldDate": None,
        "mockFieldDouble": mockFieldDouble,
        "mockFieldInt": mockFieldInt,
        "mockFieldDate": None,
        "mockFileMd5": None,
        "mockFilePath": None,
        "mockFileSize": None,
        "mockKeyLong": keyLong,
        "mockKeyString": keyString
      """)
    mapper.document(key, metadata) should be(
      json.getOrElse(Json.Null)
    )
  }
}
