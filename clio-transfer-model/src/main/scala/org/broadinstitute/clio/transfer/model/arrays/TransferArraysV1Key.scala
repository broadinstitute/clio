package org.broadinstitute.clio.transfer.model.arrays

import org.broadinstitute.clio.transfer.model.TransferKey
import org.broadinstitute.clio.util.model.Location

case class TransferArraysV1Key(
  location: Location,
  chipwellBarcode: String,
  analysisVersionNumber: Int,
  version: Int
) extends TransferKey {

  override def getUrlSegments: Seq[String] =
    Seq(
      location.entryName,
      chipwellBarcode,
      analysisVersionNumber.toString,
      version.toString
    )
}
