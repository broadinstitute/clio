package org.broadinstitute.clio.transfer.model

import io.circe.{Decoder, Encoder, Json}
import org.broadinstitute.clio.transfer.model.gvcf.{
  TransferGvcfV1Key,
  TransferGvcfV1Metadata,
  TransferGvcfV1QueryInput,
  TransferGvcfV1QueryOutput
}
import org.broadinstitute.clio.transfer.model.wgscram.{
  TransferWgsCramV1Key,
  TransferWgsCramV1Metadata,
  TransferWgsCramV1QueryInput,
  TransferWgsCramV1QueryOutput
}
import org.broadinstitute.clio.transfer.model.ubam.{
  TransferUbamV1Key,
  TransferUbamV1Metadata,
  TransferUbamV1QueryInput,
  TransferUbamV1QueryOutput
}
import org.broadinstitute.clio.util.generic.FieldMapper
import org.broadinstitute.clio.util.json.JsonSchema
import org.broadinstitute.clio.util.json.ModelAutoDerivation._

import scala.reflect.ClassTag

/**
  * Convenience class for building [[TransferIndex]] instances
  * by summoning most required fields from implicit scope.
  */
sealed abstract class SemiAutoTransferIndex[KT <: TransferKey, MT <: TransferMetadata[MT], QI <: TransferInput[
  QI
], QO](
  implicit
  schema: JsonSchema[QO],
  override val keyTag: ClassTag[KT],
  override val metadataTag: ClassTag[MT],
  override val queryInputTag: ClassTag[QI],
  override val queryOutputTag: ClassTag[QO],
  override val metadataDecoder: Decoder[MT],
  override val metadataEncoder: Encoder[MT],
  override val queryInputEncoder: Encoder[QI],
  override val queryInputDecoder: Decoder[QI],
  override val queryOutputEncoder: Encoder[QO],
  override val queryOutputDecoder: Decoder[QO],
  override val queryInputFieldMapper: FieldMapper[QI]
) extends TransferIndex {

  override type KeyType = KT
  override type MetadataType = MT
  override type QueryInputType = QI
  override type QueryOutputType = QO

  override val jsonSchema: Json = schema.toJson
}

case object GvcfIndex
    extends SemiAutoTransferIndex[
      TransferGvcfV1Key,
      TransferGvcfV1Metadata,
      TransferGvcfV1QueryInput,
      TransferGvcfV1QueryOutput
    ] {
  override val urlSegment: String = "gvcf"
  override val name: String = "Gvcf"
  override val commandName: String = "gvcf"
}

case object WgsUbamIndex
    extends SemiAutoTransferIndex[
      TransferUbamV1Key,
      TransferUbamV1Metadata,
      TransferUbamV1QueryInput,
      TransferUbamV1QueryOutput
    ] {
  override val urlSegment: String = "wgsubam"
  override val name: String = "WgsUbam"
  override val commandName: String = "wgs-ubam"
}

case object WgsCramIndex
    extends SemiAutoTransferIndex[
      TransferWgsCramV1Key,
      TransferWgsCramV1Metadata,
      TransferWgsCramV1QueryInput,
      TransferWgsCramV1QueryOutput
    ] {
  override val urlSegment: String = "wgscram"
  override val name: String = "WgsCram"
  override val commandName: String = "wgs-cram"
}

case object HybselUbamIndex
    extends SemiAutoTransferIndex[
      TransferUbamV1Key,
      TransferUbamV1Metadata,
      TransferUbamV1QueryInput,
      TransferUbamV1QueryOutput
    ] {
  override val urlSegment: String = "hybselubam"
  override val name: String = "HybselUbam"
  override val commandName: String = "hybsel-ubam"
}
