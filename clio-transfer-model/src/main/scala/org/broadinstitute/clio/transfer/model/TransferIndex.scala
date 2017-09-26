package org.broadinstitute.clio.transfer.model

/**
  * This models an index in our database. An index is a table (schema) within our database.
  */
sealed trait TransferIndex {

  /**
    * Encode the index as a url segment for making web service API calls
    *
    * @return the url path representation of this index
    */
  def urlSegment: String
}

final case class GvcfIndex() extends TransferIndex {
  override def urlSegment: String = "gvcf"
}

final case class WgsUbamIndex() extends TransferIndex {
  override def urlSegment: String = "wgsubam"
}
