package org.broadinstitute.clio.transfer.model

import io.circe.Json
import org.broadinstitute.clio.util.json.JsonSchemas

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

  def jsonSchema: Json
}

case object GvcfIndex extends TransferIndex {
  override def urlSegment: String = "gvcf"
  override def jsonSchema: Json = JsonSchemas.Gvcf
}

case object WgsUbamIndex extends TransferIndex {
  override def urlSegment: String = "wgsubam"
  override def jsonSchema: Json = JsonSchemas.WgsUbam
}
