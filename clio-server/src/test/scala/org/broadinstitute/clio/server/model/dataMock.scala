package org.broadinstitute.clio.server.model

import java.net.URI
import java.time.OffsetDateTime

import org.broadinstitute.clio.util.model.DocumentStatus

case class ModelMockKey(mockKeyLong: Long, mockKeyString: String)

case class ModelMockMetadata(
  mockFieldDouble: Option[Double],
  mockFieldInt: Option[Int],
  mockFieldDate: Option[OffsetDateTime],
  mockStringArray: Option[Seq[String]] = None,
  mockPathArray: Option[Seq[URI]] = None,
  mockDocumentStatus: Option[DocumentStatus] = None
)

case class ModelMockQueryInput(
  mockFieldDouble: Option[Double],
  mockFieldDateEnd: Option[OffsetDateTime],
  mockFieldDateStart: Option[OffsetDateTime],
  mockFieldInt: Option[Int],
  mockKeyLong: Option[Long],
  mockKeyString: Option[String]
)

case class ModelMockQueryOutput(
  mockKeyLong: Long,
  mockKeyString: String,
  mockFieldDouble: Option[Double],
  mockFieldInt: Option[Int],
  mockFieldDate: Option[OffsetDateTime],
  mockFileMd5: Option[Symbol],
  mockFilePath: Option[URI],
  mockFileSize: Option[Long],
  mockStringArray: Option[Seq[String]] = None,
  mockPathArray: Option[Seq[URI]] = None,
  mockDocumentStatus: Option[DocumentStatus] = None
)
