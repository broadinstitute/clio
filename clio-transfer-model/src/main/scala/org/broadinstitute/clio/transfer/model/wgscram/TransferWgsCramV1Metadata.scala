package org.broadinstitute.clio.transfer.model.wgscram

import java.time.OffsetDateTime

import org.broadinstitute.clio.transfer.model.TransferMetadata
import org.broadinstitute.clio.util.model.DocumentStatus

case class TransferWgsCramV1Metadata(
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
  /* These fields were originally meant to be part of the cram metrics index. */
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
  // TODO: Move these to top-level named fields in the wgs-ubam index?
  readgroupLevelMetricsFiles: Option[List[String]] = None
) extends TransferMetadata[TransferWgsCramV1Metadata] {

  override def pathsToMove: Seq[String] =
    Seq.concat(
      cramPath,
      craiPath,
      cramMd5Path,
      preAdapterSummaryMetricsPath,
      preAdapterDetailMetricsPath,
      alignmentSummaryMetricsPath,
      preBqsrDepthSMPath,
      preBqsrSelfSMPath,
      cramValidationReportPath,
      crosscheckPath,
      duplicateMetricsPath,
      gcBiasPdfPath,
      gcBiasSummaryMetricsPath,
      gcBiasDetailMetricsPath,
      insertSizeHistogramPath,
      insertSizeMetricsPath,
      qualityDistributionPdfPath,
      qualityDistributionMetricsPath,
      rawWgsMetricsPath,
      readgroupAlignmentSummaryMetricsPath,
      readgroupGcBiasPdfPath,
      readgroupGcBiasSummaryMetricsPath,
      readgroupGcBiasDetailMetricsPath,
      recalDataPath,
      baitBiasSummaryMetricsPath,
      baitBiasDetailMetricsPath,
      wgsMetricsPath
    ) ++ readgroupLevelMetricsFiles.getOrElse(Nil)

  override def pathsToDelete: Seq[String] =
    Seq.concat(cramPath, craiPath, cramMd5Path)

  override def mapMove(
    pathMapper: String => String
  ): TransferWgsCramV1Metadata = {
    this.copy(
      cramPath = cramPath.map(pathMapper),
      craiPath = craiPath.map(pathMapper),
      cramMd5Path = cramMd5Path.map(pathMapper),
      preAdapterSummaryMetricsPath =
        preAdapterSummaryMetricsPath.map(pathMapper),
      preAdapterDetailMetricsPath = preAdapterDetailMetricsPath.map(pathMapper),
      alignmentSummaryMetricsPath = alignmentSummaryMetricsPath.map(pathMapper),
      preBqsrDepthSMPath = preBqsrDepthSMPath.map(pathMapper),
      preBqsrSelfSMPath = preBqsrSelfSMPath.map(pathMapper),
      cramValidationReportPath = cramValidationReportPath.map(pathMapper),
      crosscheckPath = crosscheckPath.map(pathMapper),
      duplicateMetricsPath = duplicateMetricsPath.map(pathMapper),
      gcBiasPdfPath = gcBiasPdfPath.map(pathMapper),
      gcBiasSummaryMetricsPath = gcBiasSummaryMetricsPath.map(pathMapper),
      gcBiasDetailMetricsPath = gcBiasDetailMetricsPath.map(pathMapper),
      insertSizeHistogramPath = insertSizeHistogramPath.map(pathMapper),
      insertSizeMetricsPath = insertSizeMetricsPath.map(pathMapper),
      qualityDistributionPdfPath = qualityDistributionPdfPath.map(pathMapper),
      qualityDistributionMetricsPath =
        qualityDistributionMetricsPath.map(pathMapper),
      rawWgsMetricsPath = rawWgsMetricsPath.map(pathMapper),
      readgroupAlignmentSummaryMetricsPath =
        readgroupAlignmentSummaryMetricsPath.map(pathMapper),
      readgroupGcBiasPdfPath = readgroupGcBiasPdfPath.map(pathMapper),
      readgroupGcBiasSummaryMetricsPath =
        readgroupGcBiasSummaryMetricsPath.map(pathMapper),
      readgroupGcBiasDetailMetricsPath =
        readgroupGcBiasDetailMetricsPath.map(pathMapper),
      recalDataPath = recalDataPath.map(pathMapper),
      baitBiasSummaryMetricsPath = baitBiasSummaryMetricsPath.map(pathMapper),
      baitBiasDetailMetricsPath = baitBiasDetailMetricsPath.map(pathMapper),
      wgsMetricsPath = wgsMetricsPath.map(pathMapper),
      readgroupLevelMetricsFiles =
        readgroupLevelMetricsFiles.map(_.map(pathMapper))
    )
  }

  override def markDeleted(deletionNote: String): TransferWgsCramV1Metadata =
    this.copy(
      documentStatus = Some(DocumentStatus.Deleted),
      notes = appendNote(deletionNote)
    )
}
