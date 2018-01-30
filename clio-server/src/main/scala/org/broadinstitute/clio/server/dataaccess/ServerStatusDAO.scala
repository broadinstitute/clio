package org.broadinstitute.clio.server.dataaccess

import org.broadinstitute.clio.status.model.ClioStatus

import scala.concurrent.Future

trait ServerStatusDAO {
  def setStatus(status: ClioStatus): Future[Unit]

  def getStatus: Future[ClioStatus]
}
