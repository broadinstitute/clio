package org.broadinstitute.clio.status.model

import enumeratum._

import scala.collection.immutable.IndexedSeq

sealed abstract class ClioStatus(val level: Int) extends EnumEntry

object ClioStatus extends Enum[ClioStatus] {
  override val values: IndexedSeq[ClioStatus] = findValues

  case object NotStarted extends ClioStatus(0)

  case object Starting extends ClioStatus(1)

  case object Recovering extends ClioStatus(2)

  case object Started extends ClioStatus(3)

  case object ShuttingDown extends ClioStatus(4)

  case object ShutDown extends ClioStatus(5)

}

sealed trait SearchStatus extends EnumEntry

object SearchStatus extends Enum[SearchStatus] {
  override val values: IndexedSeq[SearchStatus] = findValues

  case object OK extends SearchStatus

  case object Error extends SearchStatus

}

case class StatusInfo(clio: ClioStatus, search: SearchStatus)

object StatusInfo {
  val Running = StatusInfo(ClioStatus.Started, SearchStatus.OK)
}

case class VersionInfo(version: String)
