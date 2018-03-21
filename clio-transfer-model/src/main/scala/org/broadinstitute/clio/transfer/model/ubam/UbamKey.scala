package org.broadinstitute.clio.transfer.model.ubam

import org.broadinstitute.clio.transfer.model.IndexKey
import org.broadinstitute.clio.util.model.Location

case class UbamKey(
  location: Location,
  flowcellBarcode: String,
  lane: Int,
  libraryName: String
) extends IndexKey {

  override def getUrlSegments: Seq[String] =
    Seq(location.entryName, flowcellBarcode, lane.toString, libraryName)
}
