package org.broadinstitute.clio.server.dataaccess

import org.broadinstitute.clio.status.model.ServerStatusInfo

import scala.concurrent.Future

trait ServerStatusDAO {
  def setStatus(status: ServerStatusInfo): Future[Unit]

  def getStatus: Future[ServerStatusInfo]
}
