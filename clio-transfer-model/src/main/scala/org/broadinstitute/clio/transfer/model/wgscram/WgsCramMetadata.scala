package org.broadinstitute.clio.transfer.model.wgscram

import java.net.URI
import java.time.OffsetDateTime
import java.util.UUID

import io.circe.Json
import io.circe.syntax._
import org.broadinstitute.clio.transfer.model.{DeliverableMetadata, Metadata}
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.{
  DataType,
  DocumentStatus,
  RegulatoryDesignation
}

object WgsCramMetadata extends ModelAutoDerivation {
  val defaults: Json = WgsCramMetadata(dataType = Option(DataType.WGS)).asJson
}

case class WgsCramMetadata(
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
  dataType: Option[DataType] = None,
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
) extends Metadata[WgsCramMetadata]
    with DeliverableMetadata[WgsCramMetadata] {

  override def pathsToDelete: Seq[URI] =
    Seq.concat(
      cramPath,
      craiPath,
      // Delete the cramPath.md5 file only if a workspaceName is defined otherwise there will be no md5
      // (foo.cram.md5 where foo.cram is cramPath)
      workspaceName.flatMap(
        _ =>
          cramPath.map { cp =>
            URI.create(s"$cp${WgsCramExtensions.Md5ExtensionAddition}")
        }
      )
    )

  // As of DSDEGP-1711, we are only delivering the cram, crai, and md5
  override def mapMove(
    pathMapper: (Option[URI], String) => Option[URI]
  ): WgsCramMetadata = {
    val movedCram = pathMapper(cramPath, WgsCramExtensions.CramExtension)
    this.copy(
      cramPath = movedCram,
      // DSDEGP-1715: We've settled on '.cram.crai' as the extension and
      // want to fixup files with just '.crai' when possible.
      craiPath = movedCram.map(
        cramUri => URI.create(s"$cramUri${WgsCramExtensions.CraiExtensionAddition}")
      )
    )
  }

  override def markDeleted(deletionNote: String): WgsCramMetadata =
    this.copy(
      documentStatus = Some(DocumentStatus.Deleted),
      notes = appendNote(deletionNote)
    )

  override def withWorkspaceName(name: String): WgsCramMetadata = {
    this.copy(
      workspaceName = Some(name)
    )
  }

  override def withDocumentStatus(
    documentStatus: Option[DocumentStatus]
  ): WgsCramMetadata =
    this.copy(
      documentStatus = documentStatus
    )
}
