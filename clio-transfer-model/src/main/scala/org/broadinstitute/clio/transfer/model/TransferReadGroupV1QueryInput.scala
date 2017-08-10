package org.broadinstitute.clio.transfer.model

import org.broadinstitute.clio.util.model.Location

import java.time.OffsetDateTime

case class TransferReadGroupV1QueryInput(
  flowcellBarcode: Option[String] = None,
  lane: Option[Int] = None,
  libraryName: Option[String] = None,
  location: Option[Location] = None,
  lcSet: Option[String] = None,
  project: Option[String] = None,
  runDateEnd: Option[OffsetDateTime] = None,
  runDateStart: Option[OffsetDateTime] = None,
  sampleAlias: Option[String] = None,
  documentStatus: Option[DocumentStatus]
)
