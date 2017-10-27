package org.broadinstitute.clio.transfer.model.wgscram

import java.net.URI
import java.time.OffsetDateTime
import java.util.UUID

import org.broadinstitute.clio.transfer.model.TransferMetadata
import org.broadinstitute.clio.util.model.DocumentStatus

case class TransferWgsCramV1Metadata(
  documentStatus: Option[DocumentStatus] = None,
  pipelineVersion: Option[Symbol] = None,
  workflowStartDate: Option[OffsetDateTime] = None,
  workflowEndDate: Option[OffsetDateTime] = None,
  cramMd5: Option[Symbol] = None,
  cramSize: Option[Long] = None,
  cramPath: Option[URI] = None,
  craiPath: Option[URI] = None,
  cramMd5Path: Option[URI] = None,
  logPath: Option[URI] = None,
  fingerprintPath: Option[URI] = None,
  cromwellId: Option[UUID] = None,
  workflowJsonPath: Option[URI] = None,
  optionsJsonPath: Option[URI] = None,
  wdlPath: Option[URI] = None,
  readgroupMd5: Option[Symbol] = None,
  workspaceName: Option[String] = None,
  notes: Option[String] = None,
  analysisFilesTxtPath: Option[URI] = None,
  /* These fields were originally meant to be part of the cram metrics index. */
  preAdapterSummaryMetricsPath: Option[URI] = None,
  preAdapterDetailMetricsPath: Option[URI] = None,
  alignmentSummaryMetricsPath: Option[URI] = None,
  bamValidationReportPath: Option[URI] = None,
  preBqsrDepthSmPath: Option[URI] = None,
  preBqsrSelfSmPath: Option[URI] = None,
  preBqsrLogPath: Option[URI] = None,
  cramValidationReportPath: Option[URI] = None,
  crosscheckPath: Option[URI] = None,
  duplicateMetricsPath: Option[URI] = None,
  fingerprintingSummaryMetricsPath: Option[URI] = None,
  fingerprintingDetailMetricsPath: Option[URI] = None,
  gcBiasPdfPath: Option[URI] = None,
  gcBiasSummaryMetricsPath: Option[URI] = None,
  gcBiasDetailMetricsPath: Option[URI] = None,
  insertSizeHistogramPath: Option[URI] = None,
  insertSizeMetricsPath: Option[URI] = None,
  qualityDistributionPdfPath: Option[URI] = None,
  qualityDistributionMetricsPath: Option[URI] = None,
  rawWgsMetricsPath: Option[URI] = None,
  readgroupAlignmentSummaryMetricsPath: Option[URI] = None,
  readgroupGcBiasPdfPath: Option[URI] = None,
  readgroupGcBiasSummaryMetricsPath: Option[URI] = None,
  readgroupGcBiasDetailMetricsPath: Option[URI] = None,
  recalDataPath: Option[URI] = None,
  baitBiasSummaryMetricsPath: Option[URI] = None,
  baitBiasDetailMetricsPath: Option[URI] = None,
  wgsMetricsPath: Option[URI] = None,
  // TODO: Move these to top-level named fields in the wgs-ubam index?
  readgroupLevelMetricsFiles: Option[List[URI]] = None
) extends TransferMetadata[TransferWgsCramV1Metadata] {

  override def pathsToMove: Seq[URI] =
    Seq.concat(
      cramPath,
      craiPath,
      cramMd5Path,
      preAdapterSummaryMetricsPath,
      preAdapterDetailMetricsPath,
      alignmentSummaryMetricsPath,
      preBqsrDepthSmPath,
      preBqsrSelfSmPath,
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

  override def pathsToDelete: Seq[URI] =
    Seq.concat(cramPath, craiPath, cramMd5Path)

  override def mapMove(
    pathMapper: Option[URI] => Option[URI]
  ): TransferWgsCramV1Metadata = {
    this.copy(
      cramPath = pathMapper(cramPath),
      craiPath = pathMapper(craiPath),
      cramMd5Path = pathMapper(cramMd5Path),
      preAdapterSummaryMetricsPath = pathMapper(preAdapterSummaryMetricsPath),
      preAdapterDetailMetricsPath = pathMapper(preAdapterDetailMetricsPath),
      alignmentSummaryMetricsPath = pathMapper(alignmentSummaryMetricsPath),
      preBqsrDepthSmPath = pathMapper(preBqsrDepthSmPath),
      preBqsrSelfSmPath = pathMapper(preBqsrSelfSmPath),
      cramValidationReportPath = pathMapper(cramValidationReportPath),
      crosscheckPath = pathMapper(crosscheckPath),
      duplicateMetricsPath = pathMapper(duplicateMetricsPath),
      gcBiasPdfPath = pathMapper(gcBiasPdfPath),
      gcBiasSummaryMetricsPath = pathMapper(gcBiasSummaryMetricsPath),
      gcBiasDetailMetricsPath = pathMapper(gcBiasDetailMetricsPath),
      insertSizeHistogramPath = pathMapper(insertSizeHistogramPath),
      insertSizeMetricsPath = pathMapper(insertSizeMetricsPath),
      qualityDistributionPdfPath = pathMapper(qualityDistributionPdfPath),
      qualityDistributionMetricsPath =
        pathMapper(qualityDistributionMetricsPath),
      rawWgsMetricsPath = pathMapper(rawWgsMetricsPath),
      readgroupAlignmentSummaryMetricsPath =
        pathMapper(readgroupAlignmentSummaryMetricsPath),
      readgroupGcBiasPdfPath = pathMapper(readgroupGcBiasPdfPath),
      readgroupGcBiasSummaryMetricsPath =
        pathMapper(readgroupGcBiasSummaryMetricsPath),
      readgroupGcBiasDetailMetricsPath =
        pathMapper(readgroupGcBiasDetailMetricsPath),
      recalDataPath = pathMapper(recalDataPath),
      baitBiasSummaryMetricsPath = pathMapper(baitBiasSummaryMetricsPath),
      baitBiasDetailMetricsPath = pathMapper(baitBiasDetailMetricsPath),
      wgsMetricsPath = pathMapper(wgsMetricsPath),
      readgroupLevelMetricsFiles = readgroupLevelMetricsFiles.map(
        _.flatMap(path => pathMapper(Some(path)))
      )
    )
  }

  override def markDeleted(deletionNote: String): TransferWgsCramV1Metadata =
    this.copy(
      documentStatus = Some(DocumentStatus.Deleted),
      notes = appendNote(deletionNote)
    )
}
