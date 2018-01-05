package org.broadinstitute.clio.util.model

import enumeratum.{Enum, EnumEntry}

import scala.collection.immutable.IndexedSeq

/** General-purpose marker for the type of sequencing job. */
sealed trait SequencingType extends EnumEntry

object SequencingType extends Enum[SequencingType] {
  override val values: IndexedSeq[SequencingType] = findValues

  case object WholeGenome extends SequencingType
  case object HybridSelection extends SequencingType
}
