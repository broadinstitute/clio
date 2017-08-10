package org.broadinstitute.clio.transfer.model

import enumeratum._
import scala.collection.immutable.IndexedSeq

sealed trait DocumentStatus extends EnumEntry

object DocumentStatus extends Enum[DocumentStatus] {
  override val values: IndexedSeq[DocumentStatus] = findValues

  case object Normal extends DocumentStatus
  case object Deleted extends DocumentStatus
}
