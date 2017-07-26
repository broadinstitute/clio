package org.broadinstitute.clio.transfer.model

case class TransferReadGroupV1Key(flowcellBarcode: String,
                                  lane: Int,
                                  libraryName: String)
