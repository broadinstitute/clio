package org.broadinstitute.clio.transfer.model

import java.time.OffsetDateTime

case class TransferReadGroupV1Metadata(
  analysisType: Option[String],
  baitIntervals: Option[String],
  dataType: Option[String],
  individualAlias: Option[String],
  initiative: Option[String],
  lcSet: Option[String],
  libraryType: Option[String],
  machineName: Option[String],
  molecularBarcodeName: Option[String],
  molecularBarcodeSequence: Option[String],
  pairedRun: Option[Boolean],
  productFamily: Option[String],
  productName: Option[String],
  productOrderId: Option[String],
  productPartNumber: Option[String],
  project: Option[String],
  readStructure: Option[String],
  researchProjectId: Option[String],
  researchProjectName: Option[String],
  rootSampleId: Option[String],
  runDate: Option[OffsetDateTime],
  runName: Option[String],
  sampleAlias: Option[String],
  sampleGender: Option[String],
  sampleId: Option[String],
  sampleLsid: Option[String],
  sampleType: Option[String],
  targetIntervals: Option[String],
  ubamMd5: Option[String],
  ubamPath: Option[String],
  ubamSize: Option[Long]
)
