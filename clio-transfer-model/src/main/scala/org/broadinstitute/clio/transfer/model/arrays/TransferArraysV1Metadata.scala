package org.broadinstitute.clio.transfer.model.arrays

import java.net.URI
import java.time.OffsetDateTime

import org.broadinstitute.clio.transfer.model.TransferMetadata
import org.broadinstitute.clio.util.model.DocumentStatus

case class TransferArraysV1Metadata(
  autocallCallRate: Option[Double] = None,
  autocallDate: Option[OffsetDateTime] = None,
  autocallGender: Option[Symbol] = None,
  autocallPf: Option[Boolean] = None,
  autocallVersion: Option[Int] = None,
  beadPoolManifestFile: Option[URI] = None,
  callRate: Option[Double] = None,
  callRateThreshold: Option[Float] = None,
  chipType: Option[String] = None,
  clusterFile: Option[URI] = None,
  controlSampleName: Option[String] = None,
  createdAt: Option[OffsetDateTime] = None,
  dbSnpVcf: Option[URI] = None,
  dbSnpVcfIndex: Option[URI] = None,
  documentStatus: Option[DocumentStatus] = None,
  extendedChipManifestFile: Option[URI] = None,
  filteredSnps: Option[Long] = None,
  fingerprintPath: Option[URI] = None,
  fingerprintingDetailMetricsPath: Option[URI] = None,
  fingerprintingSummaryMetricsPath: Option[URI] = None,
  fpGender: Option[Symbol] = None,
  genderClusterFile: Option[URI] = None,
  genderConcordancePf: Option[Boolean] = None,
  genotypeConcordanceContingencyMetrics: Option[URI] = None,
  genotypeConcordanceDetailMetrics: Option[URI] = None,
  genotypeConcordanceSummaryMetrics: Option[URI] = None,
  grnIdat: Option[URI] = None,
  gtc: Option[URI] = None,
  haplotypeDatabaseFile: Option[URI] = None,
  hetHomvarRatio: Option[Double] = None,
  hetPct: Option[Double] = None,
  imagingDate: Option[OffsetDateTime] = None,
  isLatest: Option[Boolean] = None,
  isZcalled: Option[Boolean] = None,
  modifiedAt: Option[OffsetDateTime] = None,
  notes: Option[String] = None,
  novelSnps: Option[Long] = None,
  numAutocallCalls: Option[Long] = None,
  numCalls: Option[Long] = None,
  numInDbSnp: Option[Long] = None,
  numNoCalls: Option[Long] = None,
  numSingletons: Option[Long] = None,
  p95Green: Option[Int] = None,
  p95Red: Option[Int] = None,
  paramsFile: Option[URI] = None,
  pctDbsnp: Option[Double] = None,
  pipelineVersion: Option[Symbol] = None,
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
  zcallPed: Option[URI] = None,
  zcallThresholdsFile: Option[URI] = None,
  zcallVersion: Option[Symbol] = None
) extends TransferMetadata[TransferArraysV1Metadata] {

  override def pathsToDelete: Seq[URI] =
    Seq.concat(
      beadPoolManifestFile,
      clusterFile,
      dbSnpVcf,
      dbSnpVcfIndex,
      extendedChipManifestFile,
      fingerprintPath,
      fingerprintingDetailMetricsPath,
      fingerprintingSummaryMetricsPath,
      genderClusterFile,
      genotypeConcordanceContingencyMetrics,
      genotypeConcordanceDetailMetrics,
      genotypeConcordanceSummaryMetrics,
      grnIdat,
      gtc,
      haplotypeDatabaseFile,
      paramsFile,
      redIdat,
      refDict,
      referenceFingerprint,
      referenceFingerprintIndex,
      refFasta,
      refFastaIndex,
      variantCallingDetailMetrics,
      variantCallingSummaryMetrics,
      zcallPed,
      zcallThresholdsFile
    )

  override def mapMove(
    pathMapper: (Option[URI], String) => Option[URI]
  ): TransferArraysV1Metadata = this.copy(
    beadPoolManifestFile = pathMapper(beadPoolManifestFile, ArraysExtensions.BpmExtension),
    clusterFile = pathMapper(clusterFile, ArraysExtensions.EgtExtension),
    dbSnpVcf = pathMapper(dbSnpVcf, ArraysExtensions.VcfGzExtension),
    dbSnpVcfIndex = pathMapper(dbSnpVcfIndex, ArraysExtensions.VcfGzTbiExtension),
    extendedChipManifestFile =
      pathMapper(extendedChipManifestFile, ArraysExtensions.CsvExtension),
    fingerprintPath = pathMapper(fingerprintPath, ""),
    fingerprintingDetailMetricsPath = pathMapper(fingerprintingDetailMetricsPath, ""),
    fingerprintingSummaryMetricsPath = pathMapper(fingerprintingSummaryMetricsPath, ""),
    genderClusterFile = pathMapper(genderClusterFile, ArraysExtensions.EgtExtension),
    genotypeConcordanceContingencyMetrics =
      pathMapper(genotypeConcordanceContingencyMetrics, ""),
    genotypeConcordanceDetailMetrics = pathMapper(genotypeConcordanceDetailMetrics, ""),
    genotypeConcordanceSummaryMetrics = pathMapper(genotypeConcordanceSummaryMetrics, ""),
    grnIdat = pathMapper(grnIdat, ArraysExtensions.IdatsExtension),
    gtc = pathMapper(gtc, ""),
    haplotypeDatabaseFile =
      pathMapper(haplotypeDatabaseFile, ArraysExtensions.TxtExtension),
    paramsFile = pathMapper(paramsFile, ArraysExtensions.TxtExtension),
    redIdat = pathMapper(grnIdat, ArraysExtensions.IdatsExtension),
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
    zcallThresholdsFile =
      pathMapper(zcallThresholdsFile, ArraysExtensions.EgtThresholdsTxtExtension)
  )

  override def markDeleted(deletionNote: String): TransferArraysV1Metadata =
    this.copy(
      documentStatus = Some(DocumentStatus.Deleted),
      notes = appendNote(deletionNote)
    )
}
