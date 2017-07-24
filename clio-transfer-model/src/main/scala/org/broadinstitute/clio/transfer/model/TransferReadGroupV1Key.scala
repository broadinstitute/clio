package org.broadinstitute.clio.transfer.model

case class TransferReadGroupV1Key(flowcellBarcode: String,
                                  lane: Int,
                                  libraryName: String)

case class TransferReadGroupV2Key(flowcellBarcode: String,
                                  lane: Int,
                                  libraryName: String,
                                  location: String)
