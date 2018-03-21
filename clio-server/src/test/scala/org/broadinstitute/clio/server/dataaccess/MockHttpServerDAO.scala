package org.broadinstitute.clio.server.dataaccess

import scala.concurrent.Future

class MockHttpServerDAO() extends HttpServerDAO {
  override def startup(): Future[Unit] = Future.unit

  override def enableApi(): Future[Unit] = Future.unit

  override def shutdown(): Future[Unit] = Future.unit

  override def awaitShutdown(): Unit = {}

  override def awaitShutdownInf(): Unit = {}
}
