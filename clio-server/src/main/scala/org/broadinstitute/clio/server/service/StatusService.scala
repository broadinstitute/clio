package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.ClioApp
import org.broadinstitute.clio.server.dataaccess.{
  SearchDAO,
  HttpServerDAO,
  ServerStatusDAO
}
import org.broadinstitute.clio.model.{StatusInfo, VersionInfo}

import scala.concurrent.{ExecutionContext, Future}

class StatusService private (
  serverStatusDAO: ServerStatusDAO,
  httpServerDAO: HttpServerDAO,
  searchDAO: SearchDAO
)(implicit executionContext: ExecutionContext) {
  def getStatus: Future[StatusInfo] = {
    for {
      serverStatus <- serverStatusDAO.getStatus
      elasticsearchStatus <- searchDAO.getClusterStatus
    } yield StatusInfo(serverStatus, elasticsearchStatus)
  }

  def getVersion: Future[VersionInfo] = {
    httpServerDAO.getVersion map VersionInfo
  }
}

object StatusService {
  def apply(
    app: ClioApp
  )(implicit executionContext: ExecutionContext): StatusService = {
    new StatusService(app.serverStatusDAO, app.httpServerDAO, app.searchDAO)
  }
}
