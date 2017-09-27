package org.broadinstitute.clio.transfer.model

import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

import java.time.OffsetDateTime

case class TransferWgsUbamV1QueryInput(
  flowcellBarcode: Option[String] = None,
  lane: Option[Int] = None,
  libraryName: Option[String] = None,
  location: Option[Location] = None,
  lcSet: Option[String] = None,
  project: Option[String] = None,
  runDateEnd: Option[OffsetDateTime] = None,
  runDateStart: Option[OffsetDateTime] = None,
  sampleAlias: Option[String] = None,
  documentStatus: Option[DocumentStatus] = None
)

object TransferWgsUbamV1QueryInput {
  def apply(key: TransferWgsUbamV1Key): TransferWgsUbamV1QueryInput = {
    TransferWgsUbamV1QueryInput(
      flowcellBarcode = Some(key.flowcellBarcode),
      lane = Some(key.lane),
      libraryName = Some(key.libraryName),
      location = Some(key.location)
    )
  }
}
