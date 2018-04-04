package org.broadinstitute.clio.transfer.model.arrays

import java.net.URI
import java.time.OffsetDateTime

import org.broadinstitute.clio.transfer.model.{DeliverableMetadata, Metadata}
import org.broadinstitute.clio.util.model.DocumentStatus

/* Declare Metadata fields in lexicographic order.  Metadata is the
 * set difference of the QueryOutput and Key fields.
 */
case class ArraysMetadata(
  chipType: Option[String] = None,
  clusterPath: Option[URI] = None,
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
    with DeliverableMetadata[ArraysMetadata] {

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

  /**
    * In most cases, the fields included here represent the data
    * to be delivered, but Arrays is a special case, where we need to
    * copy rather than move two delivery files (redIdat and grnIdat)
    *
    * @param pathMapper of files from source to destination URI
    * @return new metadata
    */
  override def mapMove(
    pathMapper: (Option[URI], String) => Option[URI]
  ): ArraysMetadata = this.copy(
    gtcPath = pathMapper(gtcPath, ArraysExtensions.GtcExtension),
    vcfPath = pathMapper(vcfPath, ArraysExtensions.VcfGzExtension),
    vcfIndexPath = pathMapper(vcfIndexPath, ArraysExtensions.VcfGzTbiExtension)
  )

  override def markDeleted(deletionNote: String): ArraysMetadata =
    this.copy(
      documentStatus = Some(DocumentStatus.Deleted),
      notes = appendNote(deletionNote)
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
