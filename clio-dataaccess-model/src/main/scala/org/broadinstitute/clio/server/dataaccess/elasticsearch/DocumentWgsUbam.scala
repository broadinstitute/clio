package org.broadinstitute.clio.server.dataaccess.elasticsearch

import java.net.URI

import org.broadinstitute.clio.util.model.{
  DocumentStatus,
  Location,
  RegulatoryDesignation,
  UpsertId
}
import java.time.OffsetDateTime

case class DocumentWgsUbam(
  upsertId: UpsertId,
  entityId: Symbol,
  flowcellBarcode: String,
  lane: Int,
  libraryName: String,
  location: Location,
  analysisType: Option[String] = None,
  baitIntervals: Option[String] = None,
  baitSet: Option[String] = None,
  dataType: Option[String] = None,
  individualAlias: Option[String] = None,
  initiative: Option[String] = None,
  lcSet: Option[String] = None,
  libraryType: Option[String] = None,
  machineName: Option[String] = None,
  molecularBarcodeName: Option[String] = None,
  molecularBarcodeSequence: Option[String] = None,
  pairedRun: Option[Boolean] = None,
  productFamily: Option[String] = None,
  productName: Option[String] = None,
  productOrderId: Option[String] = None,
  productPartNumber: Option[String] = None,
  project: Option[String] = None,
  readStructure: Option[String] = None,
  regulatoryDesignation: Option[RegulatoryDesignation] = None,
  researchProjectId: Option[String] = None,
  researchProjectName: Option[String] = None,
  rootSampleId: Option[String] = None,
  runDate: Option[OffsetDateTime] = None,
  runName: Option[String] = None,
  sampleAlias: Option[String] = None,
  sampleGender: Option[String] = None,
  sampleId: Option[String] = None,
  sampleLsid: Option[String] = None,
  sampleType: Option[String] = None,
  targetIntervals: Option[String] = None,
  notes: Option[String] = None,
  ubamMd5: Option[Symbol] = None,
  ubamPath: Option[URI] = None,
  ubamSize: Option[Long] = None,
  documentStatus: Option[DocumentStatus] = None
) extends ClioDocument
