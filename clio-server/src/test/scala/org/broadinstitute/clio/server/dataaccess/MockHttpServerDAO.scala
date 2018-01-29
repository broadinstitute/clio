package org.broadinstitute.clio.server.dataaccess

import scala.concurrent.Future

class MockHttpServerDAO() extends HttpServerDAO {
  override def startup(): Future[Unit] = Future.unit

  override def enableApi(): Future[Unit] = Future.unit

  override def getVersion: Future[String] =
    Future.successful(MockHttpServerDAO.VersionMock)

  override def shutdown(): Future[Unit] = Future.unit

  override def awaitShutdown(): Unit = {}

  override def awaitShutdownInf(): Unit = {}
}

object MockHttpServerDAO {
  val VersionMock = "Mock Server Version"
}
