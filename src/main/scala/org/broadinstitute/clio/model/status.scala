package org.broadinstitute.clio.model

case class ElasticsearchStatusInfo(status: String, nodes: Int, dataNodes: Int)

case class StatusInfo(clio: String, elasticsearch: ElasticsearchStatusInfo)

case class VersionInfo(version: String)
