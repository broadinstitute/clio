package org.broadinstitute.clio.server.model

import java.net.URI
import java.time.OffsetDateTime

case class ModelMockKey(mockKeyLong: Long, mockKeyString: String)

case class ModelMockMetadata(mockFieldDouble: Option[Double],
                             mockFieldInt: Option[Int],
                             mockFieldDate: Option[OffsetDateTime])

case class ModelMockQueryInput(mockFieldDouble: Option[Double],
                               mockFieldDateEnd: Option[OffsetDateTime],
                               mockFieldDateStart: Option[OffsetDateTime],
                               mockFieldInt: Option[Int],
                               mockKeyLong: Option[Long],
                               mockKeyString: Option[String])

case class ModelMockQueryOutput(mockKeyLong: Long,
                                mockKeyString: String,
                                mockFieldDouble: Option[Double],
                                mockFieldInt: Option[Int],
                                mockFieldDate: Option[OffsetDateTime],
                                mockFileMd5: Option[Symbol],
                                mockFilePath: Option[URI],
                                mockFileSize: Option[Long])
