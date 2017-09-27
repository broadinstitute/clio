package org.broadinstitute.clio.server.dataaccess.elasticsearch

import org.broadinstitute.clio.server.dataaccess.util.UpsertIdGenerator

import java.time.OffsetDateTime

case class DocumentMock(upsertId: String,
                        entityId: String,
                        mockKeyLong: Long,
                        mockKeyString: String,
                        mockFieldDate: Option[OffsetDateTime] = None,
                        mockFieldDouble: Option[Double] = None,
                        mockFieldInt: Option[Int] = None,
                        mockFileMd5: Option[String] = None,
                        mockFilePath: Option[String] = None,
                        mockFileSize: Option[Long] = None)
    extends ClioDocument

object DocumentMock {
  def default: DocumentMock = DocumentMock(
    upsertId = UpsertIdGenerator.nextId(),
    entityId = "1234.the-string",
    mockKeyLong = 1234L,
    mockKeyString = "the-string"
  )

  val index: ElasticsearchIndex[DocumentMock] =
    ElasticsearchIndex.indexDocument[DocumentMock]
}
