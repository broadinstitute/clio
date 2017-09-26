package org.broadinstitute.clio.transfer.model

import org.broadinstitute.clio.util.model.Location

case class TransferWgsUbamV1Key(flowcellBarcode: String,
                                lane: Int,
                                libraryName: String,
                                location: Location)
    extends TransferKey {

  def getUrlPath: String =
    s"$flowcellBarcode/$lane/$libraryName/$location"
}
