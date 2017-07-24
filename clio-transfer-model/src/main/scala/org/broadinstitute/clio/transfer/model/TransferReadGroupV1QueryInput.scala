package org.broadinstitute.clio.transfer.model

import java.time.OffsetDateTime

case class TransferReadGroupV1QueryInput(flowcellBarcode: Option[String],
                                         lane: Option[Int],
                                         lcSet: Option[String],
                                         libraryName: Option[String],
                                         project: Option[String],
                                         runDateEnd: Option[OffsetDateTime],
                                         runDateStart: Option[OffsetDateTime],
                                         sampleAlias: Option[String])

case class TransferReadGroupV2QueryInput(flowcellBarcode: Option[String],
                                         lane: Option[Int],
                                         libraryName: Option[String],
                                         location: Option[String],
                                         lcSet: Option[String],
                                         project: Option[String],
                                         runDateEnd: Option[OffsetDateTime],
                                         runDateStart: Option[OffsetDateTime],
                                         sampleAlias: Option[String])
