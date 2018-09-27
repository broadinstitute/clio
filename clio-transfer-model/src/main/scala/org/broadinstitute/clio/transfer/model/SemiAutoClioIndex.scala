package org.broadinstitute.clio.transfer.model

import cats.Show
import io.circe.{Decoder, Encoder, ObjectEncoder, Printer}
import io.circe.syntax._
import org.broadinstitute.clio.transfer.model.arrays.{
  ArraysKey,
  ArraysMetadata,
  ArraysQueryInput
}
import org.broadinstitute.clio.transfer.model.gvcf.{GvcfKey, GvcfMetadata, GvcfQueryInput}
import org.broadinstitute.clio.transfer.model.wgscram.{
  CramKey,
  CramMetadata,
  CramQueryInput
}
import org.broadinstitute.clio.transfer.model.ubam.{UbamKey, UbamMetadata, UbamQueryInput}
import org.broadinstitute.clio.util.generic.FieldMapper
import org.broadinstitute.clio.util.json.ModelAutoDerivation._

import scala.reflect.ClassTag

/**
  * Convenience class for building [[ClioIndex]] instances
  * by summoning most required fields from implicit scope.
  */
sealed abstract class SemiAutoClioIndex[
  KT <: IndexKey,
  MT <: Metadata[MT],
  QI <: QueryInput[QI]
](
  implicit
  override val keyTag: ClassTag[KT],
  override val metadataTag: ClassTag[MT],
  override val queryInputTag: ClassTag[QI],
  override val keyEncoder: ObjectEncoder[KT],
  override val keyDecoder: Decoder[KT],
  override val metadataDecoder: Decoder[MT],
  override val metadataEncoder: Encoder[MT],
  override val queryInputEncoder: Encoder[QI],
  override val queryInputDecoder: Decoder[QI],
  override val keyMapper: FieldMapper[KT],
  override val metadataMapper: FieldMapper[MT],
  override val queryInputMapper: FieldMapper[QI]
) extends ClioIndex {

  override type KeyType = KT
  override type MetadataType = MT
  override type QueryInputType = QI

  override val showKey: Show[KT] =
    _.asJson.pretty(Printer.noSpaces.copy(colonRight = " ", objectCommaRight = " "))
}

case object GvcfIndex
    extends SemiAutoClioIndex[
      GvcfKey,
      GvcfMetadata,
      GvcfQueryInput
    ] {
  override val urlSegment: String = "gvcf"
  override val name: String = "Gvcf"
  override val commandName: String = "gvcf"
}

case object UbamIndex
    extends SemiAutoClioIndex[
      UbamKey,
      UbamMetadata,
      UbamQueryInput
    ] {
  override val urlSegment: String = "ubam"
  override val name: String = "Ubam"
  override val commandName: String = "ubam"
}

case object CramIndex
    extends SemiAutoClioIndex[
      CramKey,
      CramMetadata,
      CramQueryInput
    ]
    with DeliverableIndex {
  override val urlSegment: String = "cram"
  override val name: String = "Cram"
  override val commandName: String = "cram"
}

case object ArraysIndex
    extends SemiAutoClioIndex[
      ArraysKey,
      ArraysMetadata,
      ArraysQueryInput
    ]
    with DeliverableIndex {
  override val urlSegment: String = "arrays"
  override val name: String = "Arrays"
  override val commandName: String = "arrays"
}
