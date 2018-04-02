package org.broadinstitute.clio.transfer.model.arrays

import org.broadinstitute.clio.transfer.model.IndexKey
import org.broadinstitute.clio.util.model.Location

case class ArraysKey(
  /*
   * Key fields declared in getUrlSegments() order
   */
  location: Location,
  chipwellBarcode: Symbol,
  version: Int
) extends IndexKey {

  override def getUrlSegments: Seq[String] =
    Seq(
      location.entryName,
      chipwellBarcode.name,
      version.toString
    )
}
