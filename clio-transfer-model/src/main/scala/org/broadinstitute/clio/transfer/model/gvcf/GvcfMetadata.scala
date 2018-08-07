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

  override def mapMove(constantBaseNameMapper: Option[URI] => Option[URI])(
    renamingMapper: (Option[URI], String) => Option[URI]
  ): GvcfMetadata = {
    this.copy(
      gvcfPath = renamingMapper(gvcfPath, GvcfExtensions.GvcfExtension),
      gvcfIndexPath = renamingMapper(gvcfIndexPath, GvcfExtensions.IndexExtension),
      gvcfSummaryMetricsPath = renamingMapper(
        gvcfSummaryMetricsPath,
        GvcfExtensions.SummaryMetricsExtension
      ),
      gvcfDetailMetricsPath =
        renamingMapper(gvcfDetailMetricsPath, GvcfExtensions.DetailMetricsExtension)
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
