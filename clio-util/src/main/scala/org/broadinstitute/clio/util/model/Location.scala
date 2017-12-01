package org.broadinstitute.clio.util.model

import enumeratum.{Enum, EnumEntry}

import scala.collection.immutable.IndexedSeq

/** General-purpose marker for the location of any object Clio cares about. */
sealed trait Location extends EnumEntry

object Location extends Enum[Location] {
  override val values: IndexedSeq[Location] = findValues

  case object GCP extends Location
  case object OnPrem extends Location
}
