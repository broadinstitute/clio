package org.broadinstitute.clio.transfer.model

import org.broadinstitute.clio.util.model.Location

case class TransferReadGroupV1Key(flowcellBarcode: String,
                                  lane: Int,
                                  libraryName: String,
                                  location: Location)
