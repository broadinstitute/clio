package org.broadinstitute.clio.transfer.model.wgscram

import java.net.URI
import java.time.OffsetDateTime
import java.util.UUID

import org.broadinstitute.clio.transfer.model.{DeliverableMetadata, Metadata}
import org.broadinstitute.clio.util.model.{DocumentStatus, RegulatoryDesignation}

case class CramMetadata(
  documentStatus: Option[DocumentStatus] = None,
  pipelineVersion: Option[Symbol] = None,
  workflowStartDate: Option[OffsetDateTime] = None,
  workflowEndDate: Option[OffsetDateTime] = None,
  cramMd5: Option[Symbol] = None,
  cramSize: Option[Long] = None,
  cramPath: Option[URI] = None,
  craiPath: Option[URI] = None,
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
  fingerprintVcfPath: Option[URI] = None,
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
  regulatoryDesignation: Option[RegulatoryDesignation] = None,
  // TODO: Move these to top-level named fields in the wgs-ubam index?
  readgroupLevelMetricsFiles: Option[List[URI]] = None
) extends Metadata[CramMetadata]
    with DeliverableMetadata[CramMetadata] {

  override def pathsToDelete: Seq[URI] =
    Seq.concat(
      cramPath,
      craiPath,
      // Delete the cramPath.md5 file only if a workspaceName is defined otherwise there will be no md5
      // (foo.cram.md5 where foo.cram is cramPath)
      workspaceName.flatMap(
        _ =>
          cramPath.map { cp =>
            URI.create(s"$cp${CramExtensions.Md5ExtensionAddition}")
        }
      )
    )

  override def mapMove(
    pathMapper: (Option[URI], String) => Option[URI]
  ): CramMetadata = {
    val movedCram = pathMapper(cramPath, CramExtensions.CramExtension)
    this.copy(
      cramPath = movedCram,
      // DSDEGP-1715: We've settled on '.cram.crai' as the extension and
      // want to fixup files with just '.crai' when possible.
      craiPath = movedCram.map(
        cramUri => URI.create(s"$cramUri${CramExtensions.CraiExtensionAddition}")
      ),
      analysisFilesTxtPath =
        pathMapper(analysisFilesTxtPath, CramExtensions.AnalysisFilesTxtExtension),
      preAdapterSummaryMetricsPath = pathMapper(
        preAdapterSummaryMetricsPath,
        CramExtensions.PreAdapterSummaryMetricsExtension
      ),
      preAdapterDetailMetricsPath = pathMapper(
        preAdapterDetailMetricsPath,
        CramExtensions.PreAdapterDetailMetricsExtension
      ),
      alignmentSummaryMetricsPath = pathMapper(
        alignmentSummaryMetricsPath,
        CramExtensions.AlignmentSummaryMetricsExtension
      ),
      // Note preBqsrDepth Metrics no longer created by workflow
      preBqsrSelfSmPath =
        pathMapper(preBqsrSelfSmPath, CramExtensions.PreBqsrSelfSMExtension),
      cramValidationReportPath = pathMapper(
        cramValidationReportPath,
        CramExtensions.CramValidationReportExtension
      ),
      crosscheckPath = pathMapper(crosscheckPath, CramExtensions.CrossCheckExtension),
      fingerprintingSummaryMetricsPath = pathMapper(
        fingerprintingSummaryMetricsPath,
        CramExtensions.FingerprintingSummaryMetricsExtension
      ),
      fingerprintingDetailMetricsPath = pathMapper(
        fingerprintingDetailMetricsPath,
        CramExtensions.FingerprintingDetailMetricsExtension
      ),
      fingerprintVcfPath =
        pathMapper(fingerprintVcfPath, CramExtensions.FingerprintVcfExtension)
    )
  }

  override def markDeleted(deletionNote: String): CramMetadata =
    this.copy(
      documentStatus = Some(DocumentStatus.Deleted),
      notes = appendNote(deletionNote)
    )

  override def withWorkspaceName(name: String): CramMetadata = {
    this.copy(
      workspaceName = Some(name)
    )
  }

  override def withDocumentStatus(
    documentStatus: Option[DocumentStatus]
  ): CramMetadata =
    this.copy(
      documentStatus = documentStatus
    )
}
