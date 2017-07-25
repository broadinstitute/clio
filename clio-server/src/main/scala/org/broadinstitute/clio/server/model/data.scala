package org.broadinstitute.clio.server.model

import java.time.OffsetDateTime

object ModelReadGroupLocation {
  sealed abstract class Place(override val toString: String)
  case object Gcp extends Place("GCP")
  case object OnPrem extends Place("OnPrem")
  val key = "location"
  val unknown = (key, "unknown")
}

case class ModelReadGroupKey(flowcellBarcode: String,
                             lane: Int,
                             libraryName: String,
                             location: String)

case class ModelReadGroupMetadata(analysisType: Option[String],
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
                                  ubamSize: Option[Long])

case class ModelReadGroupQueryInput(flowcellBarcode: Option[String],
                                    lane: Option[Int],
                                    libraryName: Option[String],
                                    location: Option[String],
                                    lcSet: Option[String],
                                    project: Option[String],
                                    runDateEnd: Option[OffsetDateTime],
                                    runDateStart: Option[OffsetDateTime],
                                    sampleAlias: Option[String])

case class ModelReadGroupQueryOutput(flowcellBarcode: String,
                                     lane: Int,
                                     libraryName: String,
                                     location: String,
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
                                     ubamSize: Option[Long])
