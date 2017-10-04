package org.broadinstitute.clio.transfer.model.wgsubam

import org.broadinstitute.clio.transfer.model.TransferKey
import org.broadinstitute.clio.util.model.Location

case class TransferWgsUbamV1Key(location: Location,
                                flowcellBarcode: String,
                                lane: Int,
                                libraryName: String)
    extends TransferKey {

  def getUrlPath: String =
    s"$location/$flowcellBarcode/$lane/$libraryName"
}

object TransferWgsUbamV1Key {
  def apply(wgsUbam: TransferWgsUbamV1QueryOutput): TransferWgsUbamV1Key =
    new TransferWgsUbamV1Key(
      wgsUbam.location,
      wgsUbam.flowcellBarcode,
      wgsUbam.lane,
      wgsUbam.libraryName
    )
}
