package org.broadinstitute.clio.server.dataaccess

import scala.concurrent.Future

class MockSearchDAO extends SearchDAO {
  override def checkOk: Future[Unit] = {
    Future.successful(())
  }

  override def initialize(): Future[Unit] = {
    Future.successful(())
  }

  override def close(): Future[Unit] = {
    Future.successful(())
  }
}
