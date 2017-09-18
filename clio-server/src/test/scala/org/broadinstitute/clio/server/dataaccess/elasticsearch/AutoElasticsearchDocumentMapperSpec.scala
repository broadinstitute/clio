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

  val keyLong = 12345L
  val keyString = "key"
  val expectedEntityId = s"$keyLong.$keyString"

  it should "empty" in {
    mapper.empty(ModelMockKey(keyLong, keyString)) should be(
      DocumentMock(
        upsertId = expectedId,
        entityId = expectedEntityId,
        mockFieldDate = None,
        mockFieldDouble = None,
        mockFieldInt = None,
        mockFileMd5 = None,
        mockFilePath = None,
        mockFileSize = None,
        mockKeyLong = keyLong,
        mockKeyString = keyString
      )
    )
  }

  it should "withMetadata" in {
    val document = DocumentMock(
      upsertId = expectedId,
      entityId = expectedEntityId,
      mockFieldDate = None,
      mockFieldDouble = Option(1.23),
      mockFieldInt = None,
      mockFileMd5 = None,
      mockFilePath = None,
      mockFileSize = None,
      mockKeyLong = keyLong,
      mockKeyString = keyString
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
        entityId = expectedEntityId,
        mockFieldDate = None,
        mockFieldDouble = Option(1.23),
        mockFieldInt = Option(456),
        mockFileMd5 = None,
        mockFilePath = None,
        mockFileSize = None,
        mockKeyLong = keyLong,
        mockKeyString = keyString
      )
    )
  }
}
