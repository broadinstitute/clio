package org.broadinstitute.clio.transfer.model

import io.circe.{Decoder, Encoder, Json}
import org.broadinstitute.clio.util.json.{JsonSchemas, ModelAutoDerivation}

/**
  * This models an index in our database. An index is a table (schema) within our database.
  */
sealed trait TransferIndex extends ModelAutoDerivation {

  /**
    * Encode the index as a url segment for making web service API calls
    *
    * @return the url path representation of this index
    */
  val urlSegment: String

  val jsonSchema: Json

  val name: String

  val commandName: String

  type MetadataType

  type QueryInputType

  type OutputType

  val metadataDecoder: Decoder[MetadataType]

  val metadataEncoder: Encoder[MetadataType]

  val queryInputEncoder: Encoder[QueryInputType]
}

case object GvcfIndex extends TransferIndex {

  override val urlSegment: String = "gvcf"

  override val jsonSchema: Json = JsonSchemas.Gvcf

  override val name: String = "Gvcf"

  override val commandName: String = "gvcf"

  override type MetadataType = TransferGvcfV1Metadata

  override type QueryInputType = TransferGvcfV1QueryInput

  override type OutputType = TransferGvcfV1QueryOutput

  override val metadataDecoder: Decoder[MetadataType] =
    Decoder[MetadataType]

  override val metadataEncoder: Encoder[MetadataType] =
    Encoder[MetadataType]

  override val queryInputEncoder: Encoder[QueryInputType] =
    Encoder[QueryInputType]
}

case object WgsUbamIndex extends TransferIndex {
  override val urlSegment: String = "wgsubam"

  override val jsonSchema: Json = JsonSchemas.WgsUbam

  override val name: String = "WgsUbam"

  override val commandName: String = "wgs-ubam"

  override type MetadataType = TransferWgsUbamV1Metadata

  override type QueryInputType = TransferWgsUbamV1QueryInput

  override type OutputType = TransferWgsUbamV1QueryOutput

  override val metadataDecoder: Decoder[MetadataType] =
    Decoder[MetadataType]

  override val metadataEncoder: Encoder[MetadataType] =
    Encoder[MetadataType]

  override val queryInputEncoder: Encoder[QueryInputType] =
    Encoder[QueryInputType]
}
