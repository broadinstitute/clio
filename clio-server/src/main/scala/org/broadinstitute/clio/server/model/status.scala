package org.broadinstitute.clio.server.model

import enumeratum._

import scala.collection.immutable.IndexedSeq

sealed trait ServerStatusInfo extends EnumEntry

object ServerStatusInfo extends Enum[ServerStatusInfo] {
  override val values: IndexedSeq[ServerStatusInfo] = findValues

  case object NotStarted extends ServerStatusInfo

  case object Starting extends ServerStatusInfo

  case object Started extends ServerStatusInfo

  case object ShuttingDown extends ServerStatusInfo

  case object ShutDown extends ServerStatusInfo

}

sealed trait SystemStatusInfo extends EnumEntry

object SystemStatusInfo extends Enum[SystemStatusInfo] {
  override val values: IndexedSeq[SystemStatusInfo] = findValues

  case object OK extends SystemStatusInfo

  case object Error extends SystemStatusInfo

}

case class StatusInfo(clio: ServerStatusInfo, search: SystemStatusInfo)

case class VersionInfo(version: String)
