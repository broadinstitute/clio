package org.broadinstitute.clio.transfer.model.gvcf

import java.net.URI
import java.time.OffsetDateTime

import org.broadinstitute.clio.util.model.{
  DocumentStatus,
  Location,
  RegulatoryDesignation
}

case class GvcfQueryOutput(
  location: Location,
  project: String,
  sampleAlias: String,
  version: Int,
  analysisDate: Option[OffsetDateTime] = None,
  contamination: Option[Float] = None,
  documentStatus: Option[DocumentStatus] = None,
  gvcfMd5: Option[Symbol] = None,
  gvcfPath: Option[URI] = None,
  gvcfSize: Option[Long] = None,
  gvcfIndexPath: Option[URI] = None,
  gvcfSummaryMetricsPath: Option[URI] = None,
  gvcfDetailMetricsPath: Option[URI] = None,
  pipelineVersion: Option[Symbol] = None,
  regulatoryDesignation: Option[RegulatoryDesignation] = None,
  notes: Option[String] = None
)
