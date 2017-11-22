package org.broadinstitute.clio.transfer.model.gvcf

import java.net.URI
import java.time.OffsetDateTime

import org.broadinstitute.clio.transfer.model.TransferMetadata
import org.broadinstitute.clio.util.model.{
  DocumentStatus,
  RegulatoryDesignation
}

case class TransferGvcfV1Metadata(
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
) extends TransferMetadata[TransferGvcfV1Metadata] {

  override def pathsToDelete: Seq[URI] =
    Seq.concat(gvcfPath, gvcfIndexPath)

  override def mapMove(
    pathMapper: (Option[URI], String) => Option[URI]
  ): TransferGvcfV1Metadata = {
    this.copy(
      gvcfPath = pathMapper(gvcfPath, ".g.vcf.gz"),
      gvcfIndexPath = pathMapper(gvcfIndexPath, ".g.vcf.gz.tbi"),
      gvcfSummaryMetricsPath =
        pathMapper(gvcfSummaryMetricsPath, ".variant_calling_summary_metrics"),
      gvcfDetailMetricsPath =
        pathMapper(gvcfDetailMetricsPath, ".variant_calling_detail_metrics")
    )
  }

  override def markDeleted(deletionNote: String): TransferGvcfV1Metadata =
    this.copy(
      documentStatus = Some(DocumentStatus.Deleted),
      notes = appendNote(deletionNote)
    )
}
