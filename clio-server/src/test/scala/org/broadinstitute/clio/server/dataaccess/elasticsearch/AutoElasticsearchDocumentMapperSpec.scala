package org.broadinstitute.clio.server.dataaccess.elasticsearch

import org.broadinstitute.clio.server.model.{ModelMockKey, ModelMockMetadata}
import org.scalatest.{FlatSpec, Matchers}

class AutoElasticsearchDocumentMapperSpec extends FlatSpec with Matchers {
  behavior of "AutoElasticsearchDocumentMapper"

  it should "id" in {
    val mapper = AutoElasticsearchDocumentMapper[
      ModelMockKey,
      ModelMockMetadata,
      DocumentMock
    ]
    mapper.id(ModelMockKey(12345L, "key")) should be("12345.key")
  }

  it should "empty" in {
    val mapper = AutoElasticsearchDocumentMapper[
      ModelMockKey,
      ModelMockMetadata,
      DocumentMock
    ]
    mapper.empty(ModelMockKey(12345L, "key")) should be(
      DocumentMock(
        mockFieldDate = None,
        mockFieldDouble = None,
        mockFieldInt = None,
        mockFileMd5 = None,
        mockFilePath = None,
        mockFileSize = None,
        mockKeyLong = 12345L,
        mockKeyString = "key"
      )
    )
  }

  it should "withMetadata" in {
    val mapper = AutoElasticsearchDocumentMapper[
      ModelMockKey,
      ModelMockMetadata,
      DocumentMock
    ]
    val document = DocumentMock(
      mockFieldDate = None,
      mockFieldDouble = Option(1.23),
      mockFieldInt = None,
      mockFileMd5 = None,
      mockFilePath = None,
      mockFileSize = None,
      mockKeyLong = 12345L,
      mockKeyString = "key"
    )
    val metadata =
      ModelMockMetadata(
        mockFieldDouble = None,
        mockFieldInt = Option(456),
        mockFieldDate = None
      )
    mapper.withMetadata(document, metadata) should be(
      DocumentMock(
        mockFieldDate = None,
        mockFieldDouble = Option(1.23),
        mockFieldInt = Option(456),
        mockFileMd5 = None,
        mockFilePath = None,
        mockFileSize = None,
        mockKeyLong = 12345L,
        mockKeyString = "key"
      )
    )
  }
}
