package org.broadinstitute.clio.transfer.model.arrays

import java.net.URI
import java.time.OffsetDateTime
import java.util.UUID

import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

/* Union of fields in Key and Metadata
 */
case class ArraysQueryOutput(
  /*
   * Key fields in getUrlSegments() order
   */
  location: Option[Location] = None,
  chipwellBarcode: Option[Symbol] = None,
  analysisVersionNumber: Option[Int] = None,
  version: Option[Int] = None,
  /*
   * rest of QueryInput fields in declaration order
   */
  cromwellId: Option[UUID] = None,
  documentStatus: Option[DocumentStatus] = None,
  notes: Option[String] = None,
  pipelineVersion: Option[Symbol] = None,
  project: Option[String] = None,
  sampleAlias: Option[String] = None,
  workflowEndDate: Option[OffsetDateTime] = None,
  workflowStartDate: Option[OffsetDateTime] = None,
  workspaceName: Option[String] = None,
  /*
   * rest of the Metadata fields in declaration order
   */
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
  dbSnpVcf: Option[URI] = None,
  dbSnpVcfIndex: Option[URI] = None,
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
  redIdat: Option[URI] = None,
  refDict: Option[URI] = None,
  refFasta: Option[URI] = None,
  refFastaIndex: Option[URI] = None,
  referenceFingerprint: Option[URI] = None,
  referenceFingerprintIndex: Option[URI] = None,
  reportedGender: Option[Symbol] = None,
  researchProjectId: Option[String] = None,
  scannerName: Option[Symbol] = None,
  totalAssays: Option[Long] = None,
  totalIndels: Option[Long] = None,
  totalSnps: Option[Long] = None,
  variantCallingDetailMetrics: Option[URI] = None,
  variantCallingSummaryMetrics: Option[URI] = None,
  zcallPed: Option[URI] = None,
  zcallThresholdsPath: Option[URI] = None,
  zcallVersion: Option[Symbol] = None
)
