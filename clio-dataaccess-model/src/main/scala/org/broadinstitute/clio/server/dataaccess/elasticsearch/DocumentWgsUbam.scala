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
  analysisType: Option[Symbol] = None,
  baitIntervals: Option[Symbol] = None,
  baitSet: Option[Symbol] = None,
  dataType: Option[Symbol] = None,
  individualAlias: Option[String] = None,
  initiative: Option[String] = None,
  lcSet: Option[Symbol] = None,
  libraryType: Option[String] = None,
  machineName: Option[Symbol] = None,
  molecularBarcodeName: Option[Symbol] = None,
  molecularBarcodeSequence: Option[Symbol] = None,
  pairedRun: Option[Boolean] = None,
  productFamily: Option[Symbol] = None,
  productName: Option[Symbol] = None,
  productOrderId: Option[Symbol] = None,
  productPartNumber: Option[Symbol] = None,
  project: Option[String] = None,
  readStructure: Option[Symbol] = None,
  regulatoryDesignation: Option[RegulatoryDesignation] = None,
  researchProjectId: Option[String] = None,
  researchProjectName: Option[String] = None,
  rootSampleId: Option[Symbol] = None,
  runDate: Option[OffsetDateTime] = None,
  runName: Option[Symbol] = None,
  sampleAlias: Option[String] = None,
  sampleGender: Option[Symbol] = None,
  sampleId: Option[Symbol] = None,
  sampleLsid: Option[Symbol] = None,
  sampleType: Option[Symbol] = None,
  targetIntervals: Option[Symbol] = None,
  notes: Option[String] = None,
  ubamMd5: Option[Symbol] = None,
  ubamPath: Option[URI] = None,
  ubamSize: Option[Long] = None,
  documentStatus: Option[DocumentStatus] = None
) extends ClioDocument
