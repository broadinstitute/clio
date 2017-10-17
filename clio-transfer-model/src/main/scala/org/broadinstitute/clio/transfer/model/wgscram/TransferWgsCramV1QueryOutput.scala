package org.broadinstitute.clio.transfer.model.wgscram

import java.time.OffsetDateTime

import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

case class TransferWgsCramV1QueryOutput(
  location: Location,
  project: String,
  sampleAlias: String,
  version: Int,
  documentStatus: Option[DocumentStatus] = None,
  pipelineVersion: Option[String] = None,
  workflowStartDate: Option[OffsetDateTime] = None,
  workflowEndDate: Option[OffsetDateTime] = None,
  cramMd5: Option[String] = None,
  cramSize: Option[Long] = None,
  cramPath: Option[String] = None,
  craiPath: Option[String] = None,
  cramMd5Path: Option[String] = None,
  logPath: Option[String] = None,
  fingerprintPath: Option[String] = None,
  cromwellId: Option[String] = None,
  workflowJsonPath: Option[String] = None,
  optionsJsonPath: Option[String] = None,
  wdlPath: Option[String] = None,
  readgroupMd5: Option[String] = None,
  workspaceName: Option[String] = None,
  notes: Option[String] = None,
  analysisFilesTxtPath: Option[String] = None,
  preAdapterSummaryMetricsPath: Option[String] = None,
  preAdapterDetailMetricsPath: Option[String] = None,
  alignmentSummaryMetricsPath: Option[String] = None,
  bamValidationReportPath: Option[String] = None,
  preBqsrDepthSMPath: Option[String] = None,
  preBqsrSelfSMPath: Option[String] = None,
  preBqsrLogPath: Option[String] = None,
  cramValidationReportPath: Option[String] = None,
  crosscheckPath: Option[String] = None,
  duplicateMetricsPath: Option[String] = None,
  fingerprintingSummaryMetricsPath: Option[String] = None,
  fingerprintingDetailMetricsPath: Option[String] = None,
  gcBiasPdfPath: Option[String] = None,
  gcBiasSummaryMetricsPath: Option[String] = None,
  gcBiasDetailMetricsPath: Option[String] = None,
  insertSizeHistogramPath: Option[String] = None,
  insertSizeMetricsPath: Option[String] = None,
  qualityDistributionPdfPath: Option[String] = None,
  qualityDistributionMetricsPath: Option[String] = None,
  rawWgsMetricsPath: Option[String] = None,
  readgroupAlignmentSummaryMetricsPath: Option[String] = None,
  readgroupGcBiasPdfPath: Option[String] = None,
  readgroupGcBiasSummaryMetricsPath: Option[String] = None,
  readgroupGcBiasDetailMetricsPath: Option[String] = None,
  recalDataPath: Option[String] = None,
  baitBiasSummaryMetricsPath: Option[String] = None,
  baitBiasDetailMetricsPath: Option[String] = None,
  wgsMetricsPath: Option[String] = None,
  readgroupLevelMetricsFiles: Option[List[String]] = None
)
