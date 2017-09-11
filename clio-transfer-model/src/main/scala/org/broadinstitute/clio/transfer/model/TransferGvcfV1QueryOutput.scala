package org.broadinstitute.clio.transfer.model

import java.time.OffsetDateTime

import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

case class TransferGvcfV1QueryOutput(
  location: Location,
  project: String,
  sampleAlias: String,
  version: Int,
  analysisDate: Option[OffsetDateTime] = None,
  contamination: Option[Float] = None,
  documentStatus: Option[DocumentStatus] = None,
  gvcfMd5: Option[String] = None,
  gvcfPath: Option[String] = None,
  gvcfSize: Option[Long] = None,
  gvcfMetricsPath: Option[String] = None,
  pipelineVersion: Option[String] = None,
  notes: Option[String] = None
)
