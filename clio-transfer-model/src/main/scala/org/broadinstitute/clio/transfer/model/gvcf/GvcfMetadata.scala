package org.broadinstitute.clio.transfer.model.gvcf

import java.net.URI
import java.time.OffsetDateTime
import java.util.UUID

import org.broadinstitute.clio.transfer.model.{CromwellWorkflowResultMetadata, Metadata}
import org.broadinstitute.clio.util.model.{DocumentStatus, RegulatoryDesignation}

case class GvcfMetadata(
  analysisDate: Option[OffsetDateTime] = None,
  cromwellId: Option[UUID] = None,
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
) extends Metadata[GvcfMetadata]
    with CromwellWorkflowResultMetadata[GvcfMetadata] {

  override def pathsToDelete: Seq[URI] =
    Seq.concat(gvcfPath, gvcfIndexPath)

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
