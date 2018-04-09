package org.broadinstitute.clio.util.model

import enumeratum.{Enum, EnumEntry}
import scala.collection.immutable.IndexedSeq

sealed trait AggregatedBy extends EnumEntry

object AggregatedBy extends Enum[AggregatedBy] {
  override val values: IndexedSeq[AggregatedBy] = findValues

  case object Squid extends AggregatedBy
  case object RP extends AggregatedBy
}
