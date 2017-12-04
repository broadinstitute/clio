package org.broadinstitute.clio.transfer.model.wgsubam

import java.net.URI
import java.time.OffsetDateTime

import org.broadinstitute.clio.transfer.model.TransferMetadata
import org.broadinstitute.clio.util.model.{DocumentStatus, RegulatoryDesignation}

case class TransferWgsUbamV1Metadata(
  analysisType: Option[String] = None,
  baitIntervals: Option[String] = None,
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
) extends TransferMetadata[TransferWgsUbamV1Metadata] {

  override def pathsToDelete: Seq[URI] = ubamPath.toSeq

  override def mapMove(
    pathMapper: (Option[URI], String) => Option[URI]
  ): TransferWgsUbamV1Metadata = {
    this.copy(ubamPath = pathMapper(ubamPath, WgsUbamExtensions.UbamExtension))
  }

  override def markDeleted(deletionNote: String): TransferWgsUbamV1Metadata =
    this.copy(
      documentStatus = Some(DocumentStatus.Deleted),
      notes = appendNote(deletionNote)
    )
}
