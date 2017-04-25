package org.broadinstitute.clio.dataaccess

import scala.concurrent.Future

class MockHttpServerDAO() extends HttpServerDAO {
  override def startup(): Future[Unit] = Future.successful(())

  override def getStatus: Future[String] = Future.successful(MockHttpServerDAO.StatusMock)

  override def getVersion: Future[String] = Future.successful(MockHttpServerDAO.VersionMock)

  override def shutdown(): Future[Unit] = Future.successful(())

  override def awaitShutdown(): Unit = {}

  override def awaitShutdownInf(): Unit = {}
}

object MockHttpServerDAO {
  val StatusMock = "Mock Server Status"
  val VersionMock = "Mock Server Version"
}
