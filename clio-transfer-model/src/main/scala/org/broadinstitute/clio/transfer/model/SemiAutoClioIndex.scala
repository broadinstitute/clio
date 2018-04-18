package org.broadinstitute.clio.transfer.model

import io.circe.{Decoder, Encoder, Json, ObjectEncoder}
import org.broadinstitute.clio.transfer.model.arrays.{
  ArraysKey,
  ArraysMetadata,
  ArraysQueryInput
}
import org.broadinstitute.clio.transfer.model.gvcf.{GvcfKey, GvcfMetadata, GvcfQueryInput}
import org.broadinstitute.clio.transfer.model.wgscram.{
  WgsCramKey,
  WgsCramMetadata,
  WgsCramQueryInput
}
import org.broadinstitute.clio.transfer.model.ubam.{UbamKey, UbamMetadata, UbamQueryInput}
import org.broadinstitute.clio.util.generic.FieldMapper
import org.broadinstitute.clio.util.json.ModelAutoDerivation._
import io.circe.syntax._
import org.broadinstitute.clio.util.model.DataType

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
  // Despite being decoupled from "v1", we append -v2 to keep ES indices consistent with GCS.
  // Since we compute GCS paths from the ES index name, inconsistency would break GCS paths.
  override val elasticsearchIndexName = "gvcf-v2"
}

case object WgsUbamIndex
    extends SemiAutoClioIndex[
      UbamKey,
      UbamMetadata,
      UbamQueryInput
    ] {
  override val urlSegment: String = "wgsubam"
  override val name: String = "WgsUbam"
  override val commandName: String = "wgs-ubam"
  override val elasticsearchIndexName: String = "wgs-ubam"
}

case object WgsCramIndex
    extends SemiAutoClioIndex[
      WgsCramKey,
      WgsCramMetadata,
      WgsCramQueryInput
    ]
    with DeliverableIndex {
  override val urlSegment: String = "wgscram"
  override val name: String = "WgsCram"
  override val commandName: String = "wgs-cram"
  // Despite being decoupled from "v1", we append -v2 to keep ES indices consistent with GCS.
  // Since we compute GCS paths from the ES index name, inconsistency would break GCS paths.
  override val elasticsearchIndexName: String = "wgs-cram-v2"
}

case object HybselUbamIndex
    extends SemiAutoClioIndex[
      UbamKey,
      UbamMetadata,
      UbamQueryInput
    ] {
  override val urlSegment: String = "hybselubam"
  override val name: String = "HybselUbam"
  override val commandName: String = "hybsel-ubam"
  override val elasticsearchIndexName: String = "hybsel-ubam"
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
  override val elasticsearchIndexName: String = "arrays"
}
