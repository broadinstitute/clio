package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.ClioApp
import org.broadinstitute.clio.server.dataaccess.{
  HttpServerDAO,
  SearchDAO,
  ServerStatusDAO
}
import org.broadinstitute.clio.status.model.{StatusInfo, SearchStatus, VersionInfo}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Try}

class StatusService private (
  serverStatusDAO: ServerStatusDAO,
  searchDAO: SearchDAO,
  httpServerDAO: HttpServerDAO
)(implicit executionContext: ExecutionContext) {

  def getStatus: Future[StatusInfo] = {
    for {
      serverStatus <- serverStatusDAO.getStatus
      searchStatus <- searchDAO.checkOk transform StatusService.toSystemStatusInfo
    } yield StatusInfo(serverStatus, searchStatus)
  }

  def getVersion: Future[VersionInfo] = {
    httpServerDAO.getVersion map VersionInfo
  }
}

object StatusService {

  def apply(
    app: ClioApp,
    httpServerDAO: HttpServerDAO
  )(implicit executionContext: ExecutionContext): StatusService = {
    new StatusService(app.serverStatusDAO, app.searchDAO, httpServerDAO)
  }

  private def toSystemStatusInfo(status: Try[_]): Try[SearchStatus] = {
    status.transform(
      _ => Success(SearchStatus.OK),
      _ => Success(SearchStatus.Error)
    )
  }
}
