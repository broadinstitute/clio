package org.broadinstitute.clio.transfer.model

import org.broadinstitute.clio.util.model.Location

/**
  * This models the key for an index. A key is the unique identifier for a document.
  */
trait TransferKey {

  /**
    * All indices distinguish on-prem files from cloud files.
    */
  val location: Location

  /**
    * Encode the index key as a URL path segment for use in web service API calls.
    *
    * @return the url path representation of this key
    */
  def getUrlPath: String
}
