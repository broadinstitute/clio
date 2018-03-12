package org.broadinstitute.clio.transfer.model.arrays

import java.net.URI
import java.time.OffsetDateTime
import java.util.UUID

import org.broadinstitute.clio.transfer.model.TransferMetadata
import org.broadinstitute.clio.util.model.DocumentStatus

/* Declare Metadata fields in lexicographic order.  Metadata is the
 * set difference of the QueryOutput and Key fields.
 */
case class TransferArraysV1Metadata(
  autocallCallRate: Option[Float] = None,
  autocallDate: Option[OffsetDateTime] = None,
  autocallGender: Option[Symbol] = None,
  autocallPf: Option[Boolean] = None,
  autocallVersion: Option[Int] = None,
  beadPoolManifestPath: Option[URI] = None,
  callRate: Option[Float] = None,
  chipType: Option[String] = None,
  clusterPath: Option[URI] = None,
  controlSampleName: Option[String] = None,
  cromwellId: Option[UUID] = None,
  dbSnpVcfPath: Option[URI] = None,
  dbSnpVcfIndexPath: Option[URI] = None,
  documentStatus: Option[DocumentStatus] = None,
  extendedChipManifestPath: Option[URI] = None,
  filteredSnps: Option[Long] = None,
  fingerprintPath: Option[URI] = None,
  fingerprintingDetailMetricsPath: Option[URI] = None,
  fingerprintingSummaryMetricsPath: Option[URI] = None,
  fpGender: Option[Symbol] = None,
  genderClusterPath: Option[URI] = None,
  genderConcordancePf: Option[Boolean] = None,
  genotypeConcordanceContingencyMetricsPath: Option[URI] = None,
  genotypeConcordanceDetailMetricsPath: Option[URI] = None,
  genotypeConcordanceSummaryMetricsPath: Option[URI] = None,
  grnIdatPath: Option[URI] = None,
  gtcPath: Option[URI] = None,
  haplotypeDatabasePath: Option[URI] = None,
  hetHomvarRatio: Option[Float] = None,
  hetPct: Option[Float] = None,
  imagingDate: Option[OffsetDateTime] = None,
  isLatest: Option[Boolean] = None,
  isZcalled: Option[Boolean] = None,
  notes: Option[String] = None,
  novelSnps: Option[Long] = None,
  numAutocallCalls: Option[Long] = None,
  numCalls: Option[Long] = None,
  numInDbSnp: Option[Long] = None,
  numNoCalls: Option[Long] = None,
  numSingletons: Option[Long] = None,
  p95Green: Option[Int] = None,
  p95Red: Option[Int] = None,
  paramsPath: Option[URI] = None,
  pctDbsnp: Option[Float] = None,
  pipelineVersion: Option[Symbol] = None,
  project: Option[String] = None,
  redIdatPath: Option[URI] = None,
  refDictPath: Option[URI] = None,
  refFastaPath: Option[URI] = None,
  refFastaIndexPath: Option[URI] = None,
  referenceFingerprintPath: Option[URI] = None,
  referenceFingerprintIndexPath: Option[URI] = None,
  reportedGender: Option[Symbol] = None,
  researchProjectId: Option[String] = None,
  sampleAlias: Option[String] = None,
  scannerName: Option[Symbol] = None,
  totalAssays: Option[Long] = None,
  totalIndels: Option[Long] = None,
  totalSnps: Option[Long] = None,
  variantCallingDetailMetricsPath: Option[URI] = None,
  variantCallingSummaryMetricsPath: Option[URI] = None,
  vcfPath: Option[URI] = None,
  vcfIndexPath: Option[URI] = None,
  workflowEndDate: Option[OffsetDateTime] = None,
  workflowStartDate: Option[OffsetDateTime] = None,
  workspaceName: Option[String] = None,
  zcallPedPath: Option[URI] = None,
  zcallThresholdsPath: Option[URI] = None,
  zcallVersion: Option[Symbol] = None
) extends TransferMetadata[TransferArraysV1Metadata] {

  /**
    * @return paths to delete
    */
  override def pathsToDelete: Seq[URI] =
    Seq.concat(
      fingerprintPath,
      fingerprintingDetailMetricsPath,
      fingerprintingSummaryMetricsPath,
      genotypeConcordanceContingencyMetricsPath,
      genotypeConcordanceDetailMetricsPath,
      genotypeConcordanceSummaryMetricsPath,
      gtcPath,
      paramsPath,
      referenceFingerprintPath,
      referenceFingerprintIndexPath,
      variantCallingDetailMetricsPath,
      variantCallingSummaryMetricsPath,
      vcfPath,
      vcfIndexPath,
      zcallPedPath
    )

  /**
    * @param pathMapper of files from source to destination URI
    * @return new metadata
    */
  override def mapMove(
    pathMapper: (Option[URI], String) => Option[URI]
  ): TransferArraysV1Metadata = this.copy(
    grnIdatPath = pathMapper(grnIdatPath, ArraysExtensions.IdatExtension),
    gtcPath = pathMapper(gtcPath, ""),
    redIdatPath = pathMapper(grnIdatPath, ArraysExtensions.IdatExtension),
    vcfPath = pathMapper(vcfPath, ArraysExtensions.VcfGzExtension),
    vcfIndexPath = pathMapper(vcfIndexPath, ArraysExtensions.VcfGzTbiExtension)
  )

  override def markDeleted(deletionNote: String): TransferArraysV1Metadata =
    this.copy(
      documentStatus = Some(DocumentStatus.Deleted),
      notes = appendNote(deletionNote)
    )
}
