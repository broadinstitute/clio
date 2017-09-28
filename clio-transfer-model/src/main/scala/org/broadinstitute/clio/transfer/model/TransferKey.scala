package org.broadinstitute.clio.transfer.model

/**
  * This models the key for an index. A key is the unique identifier for a document.
  */
trait TransferKey {

  /**
    * Encode the index key as a URL path segment for use in web service API calls.
    *
    * @return the url path representation of this key
    */
  def getUrlPath: String
}
