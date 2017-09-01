package org.broadinstitute.clio.server.dataaccess.elasticsearch

import java.time.OffsetDateTime
import java.util.UUID

case class DocumentMock(clioId: UUID,
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
  val index: ElasticsearchIndex[DocumentMock] =
    ElasticsearchIndex.indexDocument[DocumentMock]
}
