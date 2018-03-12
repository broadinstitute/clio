package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.ClioServerConfig
import org.broadinstitute.clio.server.dataaccess.{SearchDAO, ServerStatusDAO}
import org.broadinstitute.clio.status.model.{SearchStatus, StatusInfo, VersionInfo}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Try}

class StatusService private[service] (
  serverStatusDAO: ServerStatusDAO,
  searchDAO: SearchDAO
)(implicit executionContext: ExecutionContext) {

  def getStatus: Future[StatusInfo] = {
    for {
      serverStatus <- serverStatusDAO.getStatus
      searchStatus <- searchDAO.checkOk transform StatusService.toSystemStatusInfo
    } yield StatusInfo(serverStatus, searchStatus)
  }

  def getVersion: Future[VersionInfo] = {
    Future.successful(
      VersionInfo(
        ClioServerConfig.Version.value
      )
    )
  }
}

object StatusService {

  def apply(
    serverStatusDAO: ServerStatusDAO,
    searchDAO: SearchDAO
  )(implicit executionContext: ExecutionContext): StatusService = {
    new StatusService(serverStatusDAO, searchDAO)
  }

  private def toSystemStatusInfo(status: Try[_]): Try[SearchStatus] = {
    status.transform(
      _ => Success(SearchStatus.OK),
      _ => Success(SearchStatus.Error)
    )
  }
}
