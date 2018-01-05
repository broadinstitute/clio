package org.broadinstitute.clio.transfer.model.ubam

import org.broadinstitute.clio.transfer.model.TransferKey
import org.broadinstitute.clio.util.model.Location

case class TransferUbamV1Key(
  location: Location,
  flowcellBarcode: String,
  lane: Int,
  libraryName: String
) extends TransferKey {

  override def getUrlSegments: Seq[String] =
    Seq(location.entryName, flowcellBarcode, lane.toString, libraryName)
}
