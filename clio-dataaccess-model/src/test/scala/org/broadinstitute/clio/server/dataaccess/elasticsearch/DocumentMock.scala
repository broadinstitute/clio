package org.broadinstitute.clio.server.dataaccess.elasticsearch

import java.net.URI
import java.time.OffsetDateTime

import org.broadinstitute.clio.util.model.UpsertId

case class DocumentMock(upsertId: UpsertId,
                        entityId: Symbol,
                        mockKeyLong: Long,
                        mockKeyString: String,
                        mockFieldDate: Option[OffsetDateTime] = None,
                        mockFieldDouble: Option[Double] = None,
                        mockFieldInt: Option[Int] = None,
                        mockFileMd5: Option[Symbol] = None,
                        mockFilePath: Option[URI] = None,
                        mockFileSize: Option[Long] = None)
    extends ClioDocument

object DocumentMock {
  def default: DocumentMock = DocumentMock(
    upsertId = UpsertId.nextId(),
    entityId = Symbol("1234.the-string"),
    mockKeyLong = 1234L,
    mockKeyString = "the-string",
    mockFileMd5 = Some(Symbol("12345abcdefg")),
    mockFilePath = Some(URI.create("gs://the-bucket/the-path.file"))
  )

  val index: ElasticsearchIndex[DocumentMock] =
    ElasticsearchIndex.indexDocument[DocumentMock](version = 1)
}
