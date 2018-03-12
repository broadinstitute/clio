package org.broadinstitute.clio.transfer.model

import org.broadinstitute.clio.util.model.Location

/**
  * This models the key for an index. A key is the unique identifier for a document.
  */
trait TransferKey extends Product {

  /**
    * All indices distinguish on-prem files from cloud files.
    */
  val location: Location

  /**
    * The paths segments, in order, that should be encoded and used as URL path
    * segments for use in upsert requests to the server.
    */
  def getUrlSegments: Seq[String]
}
