package org.broadinstitute.clio.transfer.model.wgsubam

import java.time.OffsetDateTime

import org.broadinstitute.clio.util.model.{
  DocumentStatus,
  Location,
  RegulatoryDesignation
}

case class TransferWgsUbamV1QueryInput(
  flowcellBarcode: Option[String] = None,
  lane: Option[Int] = None,
  libraryName: Option[String] = None,
  location: Option[Location] = None,
  lcSet: Option[String] = None,
  project: Option[String] = None,
  regulatoryDesignation: Option[RegulatoryDesignation] = None,
  researchProjectId: Option[String] = None,
  runDateEnd: Option[OffsetDateTime] = None,
  runDateStart: Option[OffsetDateTime] = None,
  sampleAlias: Option[String] = None,
  documentStatus: Option[DocumentStatus] = None
)