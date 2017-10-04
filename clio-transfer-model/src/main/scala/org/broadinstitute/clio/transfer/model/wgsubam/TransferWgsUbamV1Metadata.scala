package org.broadinstitute.clio.transfer.model.wgsubam

import java.time.OffsetDateTime

import org.broadinstitute.clio.transfer.model.TransferMetadata
import org.broadinstitute.clio.util.model.DocumentStatus

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
  ubamMd5: Option[String] = None,
  ubamPath: Option[String] = None,
  ubamSize: Option[Long] = None,
  documentStatus: Option[DocumentStatus] = None
) extends TransferMetadata[TransferWgsUbamV1Metadata] {

  override def pathsToMove: Seq[String] = ubamPath.toSeq

  override def moveAllInto(destination: String): TransferWgsUbamV1Metadata = {
    if (!destination.endsWith("/")) {
      throw new Exception("Arguments to `moveAllInto` must end with '/'")
    }

    this.copy(ubamPath = ubamPath.map(moveInto(_, destination)))
  }

  override def setSinglePath(destination: String): TransferWgsUbamV1Metadata = {
    if (pathsToMove.length != 1) {
      throw new Exception(
        "`setSinglePath` called on metadata with more than one registered path"
      )
    }

    this.copy(ubamPath = Some(destination))
  }
}
