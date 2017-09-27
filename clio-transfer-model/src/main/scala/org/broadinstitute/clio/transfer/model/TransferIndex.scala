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
  def urlSegment: String

  def jsonSchema: Json

  def name: String

  def commandName: String

  type metadataType

  type queryInputType

  type outputType

  def metadataDecoder: Decoder[metadataType]

  def metadataEncoder: Encoder[metadataType]

  def queryInputEncoder: Encoder[queryInputType]
}

case object GvcfIndex extends TransferIndex {

  override def urlSegment: String = "gvcf"

  override def jsonSchema: Json = JsonSchemas.Gvcf

  override def name: String = "Gvcf"

  override def commandName: String = "gvcf"

  override type metadataType = TransferGvcfV1Metadata

  override type queryInputType = TransferGvcfV1QueryInput

  override type outputType = TransferGvcfV1QueryOutput

  override def metadataDecoder: Decoder[metadataType] =
    Decoder[metadataType]

  override def metadataEncoder: Encoder[metadataType] =
    Encoder[metadataType]

  override def queryInputEncoder: Encoder[queryInputType] =
    Encoder[queryInputType]
}

case object WgsUbamIndex extends TransferIndex {
  override def urlSegment: String = "wgsubam"

  override def jsonSchema: Json = JsonSchemas.WgsUbam

  override def name: String = "WgsUbam"

  override def commandName: String = "wgs-ubam"

  override type metadataType = TransferWgsUbamV1Metadata

  override type queryInputType = TransferWgsUbamV1QueryInput

  override type outputType = TransferWgsUbamV1QueryOutput

  override def metadataDecoder: Decoder[metadataType] =
    Decoder[metadataType]

  override def metadataEncoder: Encoder[metadataType] =
    Encoder[metadataType]

  override def queryInputEncoder: Encoder[queryInputType] =
    Encoder[queryInputType]
}
