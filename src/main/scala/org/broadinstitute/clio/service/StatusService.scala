package org.broadinstitute.clio.service

import org.broadinstitute.clio.ClioApp
import org.broadinstitute.clio.dataaccess.HttpServerDAO
import org.broadinstitute.clio.model.{StatusInfo, VersionInfo}

import scala.concurrent.{ExecutionContext, Future}

class StatusService private(httpServerDAO: HttpServerDAO)(implicit ec: ExecutionContext) {
  def getStatus: Future[StatusInfo] = {
    httpServerDAO.getStatus map StatusInfo
  }

  def getVersion: Future[VersionInfo] = {
    httpServerDAO.getVersion map VersionInfo
  }
}

object StatusService {
  def apply(app: ClioApp)(implicit ec: ExecutionContext): StatusService = {
    new StatusService(app.httpServerDAO)
  }
}
