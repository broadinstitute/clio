package org.broadinstitute.clio.dataaccess

import org.broadinstitute.clio.model.ServerStatusInfo

import scala.concurrent.Future

trait ServerStatusDAO {
  def setStatus(status: ServerStatusInfo): Future[Unit]

  def getStatus: Future[ServerStatusInfo]
}
