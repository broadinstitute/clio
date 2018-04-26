package org.broadinstitute.clio.transfer.model.gvcf

import java.net.URI
import java.time.OffsetDateTime

import org.broadinstitute.clio.transfer.model.Metadata
import org.broadinstitute.clio.util.model.{DocumentStatus, RegulatoryDesignation}

case class GvcfMetadata(
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
) extends Metadata[GvcfMetadata] {

  override def pathsToDelete: Seq[URI] =
    Seq.concat(gvcfPath, gvcfIndexPath)

  override def mapMove(
    pathMapper: (Option[URI], String) => Option[URI]
  ): GvcfMetadata = {
    this.copy(
      gvcfPath = pathMapper(gvcfPath, GvcfExtensions.GvcfExtension),
      gvcfIndexPath = pathMapper(gvcfIndexPath, GvcfExtensions.IndexExtension),
      gvcfSummaryMetricsPath = pathMapper(
        gvcfSummaryMetricsPath,
        GvcfExtensions.SummaryMetricsExtension
      ),
      gvcfDetailMetricsPath =
        pathMapper(gvcfDetailMetricsPath, GvcfExtensions.DetailMetricsExtension)
    )
  }

  override def markDeleted(deletionNote: String): GvcfMetadata =
    this.copy(
      documentStatus = Some(DocumentStatus.Deleted),
      notes = appendNote(deletionNote)
    )

  override def withDocumentStatus(
    docStatus: Option[DocumentStatus]
  ): GvcfMetadata =
    this.copy(
      documentStatus = docStatus
    )
}
