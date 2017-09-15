package org.broadinstitute.clio.server.dataaccess.elasticsearch

import org.broadinstitute.clio.server.dataaccess.util.ClioUUIDGenerator
import org.broadinstitute.clio.server.model.{ModelMockKey, ModelMockMetadata}

import org.scalatest.{FlatSpec, Matchers}

import java.util.UUID

class AutoElasticsearchDocumentMapperSpec extends FlatSpec with Matchers {
  behavior of "AutoElasticsearchDocumentMapper"

  val expectedId: UUID = ClioUUIDGenerator.getUUID()
  val mapper = new AutoElasticsearchDocumentMapper[
    ModelMockKey,
    ModelMockMetadata,
    DocumentMock
  ](() => expectedId)

  it should "empty" in {
    mapper.empty(ModelMockKey(12345L, "key")) should be(
      DocumentMock(
        upsertId = expectedId,
        entityId = "12345.key",
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
    val document = DocumentMock(
      upsertId = expectedId,
      entityId = "12345.key",
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
        upsertId = expectedId,
        entityId = "12345.key",
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
