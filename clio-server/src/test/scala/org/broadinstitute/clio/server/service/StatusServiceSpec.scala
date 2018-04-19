package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.dataaccess._
import org.broadinstitute.clio.status.model.{ClioStatus, SearchStatus, StatusInfo, VersionInfo}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{AsyncFlatSpec, Matchers}

import scala.concurrent.Future

class StatusServiceSpec extends AsyncFlatSpec with Matchers with MockFactory {
  behavior of "StatusService"

  val statusDAO: ServerStatusDAO = mock[ServerStatusDAO]
  val searchDAO: SearchDAO = mock[SearchDAO]
  val statusService = new StatusService(statusDAO, searchDAO)

  it should "getVersion" in {
    for {
      version <- statusService.getVersion
      _ = version should be(VersionInfo(MockServerStatusDAO.VersionMock))
    } yield succeed
  }

  it should "getStatus" in {
    val expectedServerStatus = ClioStatus.Started
    (statusDAO.getStatus _).expects().returning(Future.successful(expectedServerStatus))
    (searchDAO.checkOk _).expects().returning(Future.successful(()))
    for {
      status <- statusService.getStatus
      _ = status should be(
        StatusInfo(expectedServerStatus, SearchStatus.OK)
      )
    } yield succeed
  }
}
