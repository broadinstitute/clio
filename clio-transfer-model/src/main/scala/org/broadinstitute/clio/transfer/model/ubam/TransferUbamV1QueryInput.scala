package org.broadinstitute.clio.transfer.model.ubam

import java.time.OffsetDateTime

import org.broadinstitute.clio.transfer.model.TransferQueryInput
import org.broadinstitute.clio.util.model.{
  DocumentStatus,
  Location,
  RegulatoryDesignation
}

case class TransferUbamV1QueryInput(
  flowcellBarcode: Option[String] = None,
  lane: Option[Int] = None,
  libraryName: Option[String] = None,
  location: Option[Location] = None,
  lcSet: Option[Symbol] = None,
  project: Option[String] = None,
  regulatoryDesignation: Option[RegulatoryDesignation] = None,
  researchProjectId: Option[String] = None,
  runDateEnd: Option[OffsetDateTime] = None,
  runDateStart: Option[OffsetDateTime] = None,
  sampleAlias: Option[String] = None,
  documentStatus: Option[DocumentStatus] = None,
  baitSet: Option[Symbol] = None,
  baitIntervals: Option[Symbol] = None,
  targetIntervals: Option[Symbol] = None
) extends TransferQueryInput[TransferUbamV1QueryInput] {

  def withDocumentStatus(
    documentStatus: Option[DocumentStatus]
  ): TransferUbamV1QueryInput =
    this.copy(
      documentStatus = documentStatus
    )
}
