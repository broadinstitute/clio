package org.broadinstitute.clio.util.model

import enumeratum.{Enum, EnumEntry}
import scala.collection.immutable.IndexedSeq

sealed trait DataType extends EnumEntry

object DataType extends Enum[DataType] {
  override val values: IndexedSeq[DataType] = findValues

  case object WGS extends DataType
  case object Exome extends DataType
}