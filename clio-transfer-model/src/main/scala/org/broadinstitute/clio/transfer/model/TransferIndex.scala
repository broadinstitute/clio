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

  def decoder: Decoder[metadataType]
  def encoder: Encoder[metadataType]
}

case object GvcfIndex extends TransferIndex {

  override def urlSegment: String = "gvcf"

  override def jsonSchema: Json = JsonSchemas.Gvcf

  override def name: String = "Gvcf"

  override def commandName: String = "gvcf"

  override type metadataType = TransferGvcfV1Metadata

  override def decoder: Decoder[TransferGvcfV1Metadata] =
    Decoder[TransferGvcfV1Metadata]

  override def encoder: Encoder[TransferGvcfV1Metadata] =
    Encoder[TransferGvcfV1Metadata]
}

case object WgsUbamIndex extends TransferIndex {
  override def urlSegment: String = "wgsubam"

  override def jsonSchema: Json = JsonSchemas.WgsUbam

  override def name: String = "WgsUbam"

  override def commandName: String = "wgs-ubam"

  override type metadataType = TransferWgsUbamV1Metadata

  override def decoder: Decoder[TransferWgsUbamV1Metadata] =
    Decoder[TransferWgsUbamV1Metadata]

  override def encoder: Encoder[TransferWgsUbamV1Metadata] =
    Encoder[TransferWgsUbamV1Metadata]
}
