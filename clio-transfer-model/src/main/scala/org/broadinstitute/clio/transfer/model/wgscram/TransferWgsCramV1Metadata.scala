package org.broadinstitute.clio.transfer.model.wgscram

import java.time.OffsetDateTime

import org.broadinstitute.clio.transfer.model.TransferMetadata
import org.broadinstitute.clio.util.model.DocumentStatus

case class TransferWgsCramV1Metadata(
  documentStatus: Option[DocumentStatus] = None,
  pipelineVersion: Option[Long] = None,
  workflowStartDate: Option[OffsetDateTime] = None,
  workflowEndDate: Option[OffsetDateTime] = None,
  cramMd5: Option[String] = None,
  cramSize: Option[Long] = None,
  cramPath: Option[String] = None,
  craiPath: Option[String] = None,
  cramMd5Path: Option[String] = None,
  logPath: Option[String] = None,
  fingerprintPath: Option[String] = None,
  cromwellIdPath: Option[String] = None,
  workflowJsonPath: Option[String] = None,
  optionsJsonPath: Option[String] = None,
  wdlPath: Option[String] = None,
  readgroupMd5: Option[String] = None,
  notes: Option[String] = None,
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

  override def moveAllInto(destination: String): TransferWgsCramV1Metadata = {
    if (!destination.endsWith("/")) {
      throw new Exception("Arguments to `moveAllInto` must end with '/'")
    }

    this.copy(
      cramPath = cramPath.map(moveInto(_, destination)),
      craiPath = craiPath.map(moveInto(_, destination)),
      cramMd5Path = cramMd5Path.map(moveInto(_, destination)),
      preAdapterSummaryMetricsPath =
        preAdapterSummaryMetricsPath.map(moveInto(_, destination)),
      preAdapterDetailMetricsPath =
        preAdapterDetailMetricsPath.map(moveInto(_, destination)),
      alignmentSummaryMetricsPath =
        alignmentSummaryMetricsPath.map(moveInto(_, destination)),
      preBqsrDepthSMPath = preBqsrDepthSMPath.map(moveInto(_, destination)),
      preBqsrSelfSMPath = preBqsrSelfSMPath.map(moveInto(_, destination)),
      cramValidationReportPath =
        cramValidationReportPath.map(moveInto(_, destination)),
      crosscheckPath = crosscheckPath.map(moveInto(_, destination)),
      duplicateMetricsPath = duplicateMetricsPath.map(moveInto(_, destination)),
      gcBiasPdfPath = gcBiasPdfPath.map(moveInto(_, destination)),
      gcBiasSummaryMetricsPath =
        gcBiasSummaryMetricsPath.map(moveInto(_, destination)),
      gcBiasDetailMetricsPath =
        gcBiasDetailMetricsPath.map(moveInto(_, destination)),
      insertSizeHistogramPath =
        insertSizeHistogramPath.map(moveInto(_, destination)),
      insertSizeMetricsPath =
        insertSizeMetricsPath.map(moveInto(_, destination)),
      qualityDistributionPdfPath =
        qualityDistributionPdfPath.map(moveInto(_, destination)),
      qualityDistributionMetricsPath =
        qualityDistributionMetricsPath.map(moveInto(_, destination)),
      rawWgsMetricsPath = rawWgsMetricsPath.map(moveInto(_, destination)),
      readgroupAlignmentSummaryMetricsPath =
        readgroupAlignmentSummaryMetricsPath.map(moveInto(_, destination)),
      readgroupGcBiasPdfPath =
        readgroupGcBiasPdfPath.map(moveInto(_, destination)),
      readgroupGcBiasSummaryMetricsPath =
        readgroupGcBiasSummaryMetricsPath.map(moveInto(_, destination)),
      readgroupGcBiasDetailMetricsPath =
        readgroupGcBiasDetailMetricsPath.map(moveInto(_, destination)),
      recalDataPath = recalDataPath.map(moveInto(_, destination)),
      baitBiasSummaryMetricsPath =
        baitBiasSummaryMetricsPath.map(moveInto(_, destination)),
      baitBiasDetailMetricsPath =
        baitBiasDetailMetricsPath.map(moveInto(_, destination)),
      wgsMetricsPath = wgsMetricsPath.map(moveInto(_, destination)),
      readgroupLevelMetricsFiles =
        readgroupLevelMetricsFiles.map(_.map(moveInto(_, destination)))
    )
  }

  override def setSinglePath(destination: String): TransferWgsCramV1Metadata = {
    throw new Exception(s"`setSinglePath` not implemented for $getClass")
  }

  override def markDeleted(deletionNote: String): TransferWgsCramV1Metadata =
    this.copy(
      documentStatus = Some(DocumentStatus.Deleted),
      notes = appendNote(deletionNote)
    )
}
