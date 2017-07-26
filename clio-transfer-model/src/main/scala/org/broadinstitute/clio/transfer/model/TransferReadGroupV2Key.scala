package org.broadinstitute.clio.transfer.model

import enumeratum._
import scala.collection.immutable.IndexedSeq

sealed trait TransferReadGroupLocation extends EnumEntry

object TransferReadGroupLocation extends Enum[TransferReadGroupLocation] {
  override val values: IndexedSeq[TransferReadGroupLocation] = findValues

  case object Unknown extends TransferReadGroupLocation
  case object Gcp extends TransferReadGroupLocation
  case object OnPrem extends TransferReadGroupLocation

  val pathMatcher = Map(Gcp.toString -> Gcp, OnPrem.toString -> OnPrem)
  val key = "location"
  val unknown = (key, Unknown)
}

case class TransferReadGroupV2Key(flowcellBarcode: String,
                                  lane: Int,
                                  libraryName: String,
                                  location: TransferReadGroupLocation)
