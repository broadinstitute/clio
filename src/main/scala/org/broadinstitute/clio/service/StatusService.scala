package org.broadinstitute.clio.service

import org.broadinstitute.clio.ClioApp
import org.broadinstitute.clio.dataaccess.{ElasticsearchDAO, HttpServerDAO, ServerStatusDAO}
import org.broadinstitute.clio.model.{StatusInfo, VersionInfo}

import scala.concurrent.{ExecutionContext, Future}

class StatusService private(serverStatusDAO: ServerStatusDAO, httpServerDAO: HttpServerDAO,
                            elasticsearchDAO: ElasticsearchDAO)(implicit ec: ExecutionContext) {
  def getStatus: Future[StatusInfo] = {
    for {
      serverStatus <- serverStatusDAO.getStatus
      elasticsearchStatus <- elasticsearchDAO.getClusterStatus
    } yield StatusInfo(serverStatus, elasticsearchStatus)
  }

  def getVersion: Future[VersionInfo] = {
    httpServerDAO.getVersion map VersionInfo
  }
}

object StatusService {
  def apply(app: ClioApp)(implicit ec: ExecutionContext): StatusService = {
    new StatusService(app.serverStatusDAO, app.httpServerDAO, app.elasticsearchDAO)
  }
}
