package org.broadinstitute.clio.model

case class ServerStatusInfo(status: String)

object ServerStatusInfo {
  val NotStarted = new ServerStatusInfo("not_started")
  val Starting = new ServerStatusInfo("starting")
  val Started = new ServerStatusInfo("started")
  val ShuttingDown = new ServerStatusInfo("shutting_down")
  val ShutDown = new ServerStatusInfo("shut_down")
}

case class ElasticsearchStatusInfo(status: String, nodes: Int, dataNodes: Int)

case class StatusInfo(clio: ServerStatusInfo, elasticsearch: ElasticsearchStatusInfo)

case class VersionInfo(version: String)
