package org.broadinstitute.clio.transfer.model.arrays

import java.net.URI
import java.time.OffsetDateTime
import java.util.UUID

import org.broadinstitute.clio.transfer.model.{
  CromwellWorkflowResultMetadata,
  DeliverableMetadata,
  Metadata
}
import org.broadinstitute.clio.util.model.{DocumentStatus, RegulatoryDesignation}

/* Declare Metadata fields in lexicographic order.  Metadata is the
 * set difference of the QueryOutput and Key fields.
 */
case class ArraysMetadata(
  chipType: Option[String] = None,
  clusterPath: Option[URI] = None,
  cromwellId: Option[UUID] = None,
  documentStatus: Option[DocumentStatus] = None,
  extendedChipManifestPath: Option[URI] = None,
  fingerprintingDetailMetricsPath: Option[URI] = None,
  fingerprintingSummaryMetricsPath: Option[URI] = None,
  genderClusterPath: Option[URI] = None,
  genotypeConcordanceContingencyMetricsPath: Option[URI] = None,
  genotypeConcordanceDetailMetricsPath: Option[URI] = None,
  genotypeConcordanceSummaryMetricsPath: Option[URI] = None,
  grnIdatPath: Option[URI] = None,
  gtcPath: Option[URI] = None,
  isZcalled: Option[Boolean] = None,
  notes: Option[String] = None,
  paramsPath: Option[URI] = None,
  pipelineVersion: Option[Symbol] = None,
  project: Option[String] = None,
  redIdatPath: Option[URI] = None,
  refFastaPath: Option[URI] = None,
  refFastaIndexPath: Option[URI] = None,
  refDictPath: Option[URI] = None,
  regulatoryDesignation: Option[RegulatoryDesignation] = None,
  researchProjectId: Option[String] = None,
  sampleAlias: Option[String] = None,
  variantCallingDetailMetricsPath: Option[URI] = None,
  variantCallingSummaryMetricsPath: Option[URI] = None,
  vcfPath: Option[URI] = None,
  vcfIndexPath: Option[URI] = None,
  workflowStartDate: Option[OffsetDateTime] = None,
  workflowEndDate: Option[OffsetDateTime] = None,
  workspaceName: Option[String] = None
) extends Metadata[ArraysMetadata]
    with DeliverableMetadata[ArraysMetadata]
    with CromwellWorkflowResultMetadata[ArraysMetadata] {

  /**
    * @return paths to delete
    */
  override def pathsToDelete: Seq[URI] =
    Seq.concat(
      gtcPath,
      paramsPath,
      vcfPath,
      vcfIndexPath,
    )

  override def markDeleted(deletionNote: String): ArraysMetadata =
    this.copy(
      documentStatus = Some(DocumentStatus.Deleted),
      notes = appendNote(deletionNote)
    )

  override def markExternallyHosted(relinquishNote: String): ArraysMetadata =
    this.copy(
      documentStatus = Some(DocumentStatus.ExternallyHosted),
      notes = appendNote(relinquishNote)
    )

  override def withWorkspaceName(name: String): ArraysMetadata = {
    this.copy(
      workspaceName = Some(name)
    )
  }

  override def withDocumentStatus(
    documentStatus: Option[DocumentStatus]
  ): ArraysMetadata =
    this.copy(
      documentStatus = documentStatus
    )
}
