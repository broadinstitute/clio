package org.broadinstitute.clio.transfer.model.wgscram

import java.io.File
import java.net.URI
import java.time.OffsetDateTime
import java.util.UUID

import org.broadinstitute.clio.transfer.model.TransferMetadata
import org.broadinstitute.clio.util.model.{
  DocumentStatus,
  RegulatoryDesignation
}

case class TransferWgsCramV1Metadata(
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
) extends TransferMetadata[TransferWgsCramV1Metadata] {

  // Delete the cramPath.md5 file (foo.cram.md5 where foo.cram is cramPath).
  override def pathsToDelete: Seq[URI] =
    Seq.concat(cramPath, craiPath, cramPath.map(cp => URI.create(s"$cp.md5")))

  // As of DSDEGP-1711, we are only delivering the cram, crai, and md5
  override def mapMove(
    pathMapper: Option[URI] => Option[URI]
  ): TransferWgsCramV1Metadata = {
    this.copy(
      cramPath = pathMapper(cramPath),
      // DSDEGP-1715: We've settled on '.cram.crai' as the extension and
      // want to fixup files with just '.crai' when possible.
      craiPath = pathMapper(cramPath).map { cramUri =>
        URI.create(s"${cramUri.toString}.crai")
      }
    )
  }

  override def markDeleted(deletionNote: String): TransferWgsCramV1Metadata =
    this.copy(
      documentStatus = Some(DocumentStatus.Deleted),
      notes = appendNote(deletionNote)
    )

  override def prefixed(newPrefix: String) = {
    val prefixedCram = cramPath.map { cp =>
      val name = new File(cp.getPath).getName
      val directory = URI.create(cp.toString.dropRight(name.length))
      directory.resolve(s"$newPrefix$name")
    }
    val prefixedCrai = prefixedCram.map(pc => URI.create(s"${pc}.crai"))
    this.copy(cramPath = prefixedCram, craiPath = prefixedCrai)
  }
}
