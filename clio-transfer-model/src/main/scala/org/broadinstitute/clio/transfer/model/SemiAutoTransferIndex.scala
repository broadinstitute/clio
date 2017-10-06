package org.broadinstitute.clio.transfer.model

import io.circe.{Decoder, Encoder, Json}
import org.broadinstitute.clio.transfer.model.gvcf.{
  TransferGvcfV1Key,
  TransferGvcfV1Metadata,
  TransferGvcfV1QueryInput,
  TransferGvcfV1QueryOutput
}
import org.broadinstitute.clio.transfer.model.wgsubam.{
  TransferWgsUbamV1Key,
  TransferWgsUbamV1Metadata,
  TransferWgsUbamV1QueryInput,
  TransferWgsUbamV1QueryOutput
}
import org.broadinstitute.clio.util.json.JsonSchemas
import org.broadinstitute.clio.util.json.ModelAutoDerivation._

import scala.reflect.ClassTag

/**
  * Convenience class for building [[TransferIndex]] instances
  * by summoning most required fields from implicit scope.
  */
sealed abstract class SemiAutoTransferIndex[KT <: TransferKey,
                                            MT <: TransferMetadata[MT],
                                            QI,
                                            QO](
  implicit override val keyTag: ClassTag[KT],
  override val metadataTag: ClassTag[MT],
  override val queryInputTag: ClassTag[QI],
  override val queryOutputTag: ClassTag[QO],
  override val metadataDecoder: Decoder[MT],
  override val metadataEncoder: Encoder[MT],
  override val queryInputEncoder: Encoder[QI],
  override val queryOutputDecoder: Decoder[QO]
) extends TransferIndex {

  override type KeyType = KT
  override type MetadataType = MT
  override type QueryInputType = QI
  override type QueryOutputType = QO
}

case object GvcfIndex
    extends SemiAutoTransferIndex[
      TransferGvcfV1Key,
      TransferGvcfV1Metadata,
      TransferGvcfV1QueryInput,
      TransferGvcfV1QueryOutput
    ] {
  override val urlSegment: String = "gvcf"
  override val jsonSchema: Json = JsonSchemas.Gvcf
  override val name: String = "Gvcf"
  override val commandName: String = "gvcf"
}

case object WgsUbamIndex
    extends SemiAutoTransferIndex[
      TransferWgsUbamV1Key,
      TransferWgsUbamV1Metadata,
      TransferWgsUbamV1QueryInput,
      TransferWgsUbamV1QueryOutput
    ] {
  override val urlSegment: String = "wgsubam"
  override val jsonSchema: Json = JsonSchemas.WgsUbam
  override val name: String = "WgsUbam"
  override val commandName: String = "wgs-ubam"
}

case object WgsCramIndex
    extends SemiAutoTransferIndex[
      TransferWgsUbamV1Key,
      TransferWgsUbamV1Metadata,
      TransferWgsUbamV1QueryInput,
      TransferWgsUbamV1QueryOutput
    ] {
  override val urlSegment: String = "wgscram"
  override val jsonSchema: Json = JsonSchemas.WgsCram
  override val name: String = "WgsUbam"
  override val commandName: String = "wgs-ubam"
}
