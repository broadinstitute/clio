package org.broadinstitute.clio.server.dataaccess.elasticsearch

import java.time.OffsetDateTime
import java.util.UUID

case class DocumentMock(mockClioId: Option[UUID],
                        mockFieldDate: Option[OffsetDateTime],
                        mockFieldDouble: Option[Double],
                        mockFieldInt: Option[Int],
                        mockFileMd5: Option[String],
                        mockFilePath: Option[String],
                        mockFileSize: Option[Long],
                        mockKeyLong: Long,
                        mockKeyString: String)
