package org.broadinstitute.clio.transfer.model.arrays

import java.net.URI
import java.time.OffsetDateTime
import java.util.UUID

import org.broadinstitute.clio.transfer.model.{DeliverableMetadata, Metadata}
import org.broadinstitute.clio.util.model.DocumentStatus

/* Declare Metadata fields in lexicographic order.  Metadata is the
 * set difference of the QueryOutput and Key fields.
 */
case class ArraysMetadata(
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
  dbSnpVcf: Option[URI] = None,
  dbSnpVcfIndex: Option[URI] = None,
  documentStatus: Option[DocumentStatus] = None,
  extendedChipManifestPath: Option[URI] = None,
  filteredSnps: Option[Long] = None,
  fingerprintPath: Option[URI] = None,
  fingerprintingDetailMetricsPath: Option[URI] = None,
  fingerprintingSummaryMetricsPath: Option[URI] = None,
  fpGender: Option[Symbol] = None,
  genderClusterPath: Option[URI] = None,
  genderConcordancePf: Option[Boolean] = None,
  genotypeConcordanceContingencyMetrics: Option[URI] = None,
  genotypeConcordanceDetailMetrics: Option[URI] = None,
  genotypeConcordanceSummaryMetrics: Option[URI] = None,
  grnIdat: Option[URI] = None,
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
  redIdat: Option[URI] = None,
  refDict: Option[URI] = None,
  refFasta: Option[URI] = None,
  refFastaIndex: Option[URI] = None,
  referenceFingerprint: Option[URI] = None,
  referenceFingerprintIndex: Option[URI] = None,
  reportedGender: Option[Symbol] = None,
  researchProjectId: Option[String] = None,
  sampleAlias: Option[String] = None,
  scannerName: Option[Symbol] = None,
  totalAssays: Option[Long] = None,
  totalIndels: Option[Long] = None,
  totalSnps: Option[Long] = None,
  variantCallingDetailMetrics: Option[URI] = None,
  variantCallingSummaryMetrics: Option[URI] = None,
  workflowEndDate: Option[OffsetDateTime] = None,
  workflowStartDate: Option[OffsetDateTime] = None,
  workspaceName: Option[String] = None,
  zcallPed: Option[URI] = None,
  zcallThresholdsPath: Option[URI] = None,
  zcallVersion: Option[Symbol] = None

) extends Metadata[ArraysMetadata] with DeliverableMetadata[ArraysMetadata] {

  /**
    * FIXME: Deletes everything now, but Gvcf and WgsCram do not
    * delete everything.
    *
    * @return paths to delete
    */
  override def pathsToDelete: Seq[URI] =
    Seq.concat(
      beadPoolManifestPath,
      clusterPath,
      dbSnpVcf,
      dbSnpVcfIndex,
      extendedChipManifestPath,
      fingerprintPath,
      fingerprintingDetailMetricsPath,
      fingerprintingSummaryMetricsPath,
      genderClusterPath,
      genotypeConcordanceContingencyMetrics,
      genotypeConcordanceDetailMetrics,
      genotypeConcordanceSummaryMetrics,
      grnIdat,
      gtcPath,
      haplotypeDatabasePath,
      paramsPath,
      redIdat,
      refDict,
      referenceFingerprint,
      referenceFingerprintIndex,
      refFasta,
      refFastaIndex,
      variantCallingDetailMetrics,
      variantCallingSummaryMetrics,
      zcallPed,
      zcallThresholdsPath
    )

  /**
    * FIXME: Includes everything now, which again is not done for
    * other indexes.
    *
    * @param pathMapper of files from source to destination URI
    * @return new metadata
    */
  override def mapMove(
    pathMapper: (Option[URI], String) => Option[URI]
  ): ArraysMetadata = this.copy(
    beadPoolManifestPath = pathMapper(beadPoolManifestPath, ArraysExtensions.BpmExtension),
    clusterPath = pathMapper(clusterPath, ArraysExtensions.EgtExtension),
    dbSnpVcf = pathMapper(dbSnpVcf, ArraysExtensions.VcfGzExtension),
    dbSnpVcfIndex = pathMapper(dbSnpVcfIndex, ArraysExtensions.VcfGzTbiExtension),
    extendedChipManifestPath =
      pathMapper(extendedChipManifestPath, ArraysExtensions.CsvExtension),
    fingerprintPath = pathMapper(fingerprintPath, ""),
    fingerprintingDetailMetricsPath = pathMapper(fingerprintingDetailMetricsPath, ""),
    fingerprintingSummaryMetricsPath = pathMapper(fingerprintingSummaryMetricsPath, ""),
    genderClusterPath = pathMapper(genderClusterPath, ArraysExtensions.EgtExtension),
    genotypeConcordanceContingencyMetrics =
      pathMapper(genotypeConcordanceContingencyMetrics, ""),
    genotypeConcordanceDetailMetrics = pathMapper(genotypeConcordanceDetailMetrics, ""),
    genotypeConcordanceSummaryMetrics = pathMapper(genotypeConcordanceSummaryMetrics, ""),
    grnIdat = pathMapper(grnIdat, ArraysExtensions.IdatExtension),
    gtcPath = pathMapper(gtcPath, ""),
    haplotypeDatabasePath =
      pathMapper(haplotypeDatabasePath, ArraysExtensions.TxtExtension),
    paramsPath = pathMapper(paramsPath, ArraysExtensions.TxtExtension),
    redIdat = pathMapper(grnIdat, ArraysExtensions.IdatExtension),
    refDict = pathMapper(refDict, ArraysExtensions.DictExtension),
    referenceFingerprint =
      pathMapper(referenceFingerprint, ArraysExtensions.VcfGzExtension),
    referenceFingerprintIndex =
      pathMapper(referenceFingerprintIndex, ArraysExtensions.VcfGzTbiExtension),
    refFasta = pathMapper(refFasta, ArraysExtensions.FastaExtension),
    refFastaIndex = pathMapper(refFastaIndex, ArraysExtensions.FastaFaiExtension),
    variantCallingDetailMetrics = pathMapper(variantCallingDetailMetrics, ""),
    variantCallingSummaryMetrics = pathMapper(variantCallingSummaryMetrics, ""),
    zcallPed = pathMapper(zcallPed, ""),
    zcallThresholdsPath =
      pathMapper(zcallThresholdsPath, ArraysExtensions.EgtThresholdsTxtExtension)
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

  def withMovedIdats(grnIdatPath: URI, redIdatPath: URI): ArraysMetadata = {
    this.copy(
      grnIdat = Some(grnIdatPath),
      redIdat = Some(redIdatPath)
    )
  }

  override def withDocumentStatus(
    documentStatus: Option[DocumentStatus]
  ): ArraysMetadata =
    this.copy(
      documentStatus = documentStatus
    )
}
