package org.broadinstitute.clio.server.dataaccess

import org.broadinstitute.clio.server.model.ElasticsearchStatusInfo

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

object MockSearchDAO {
  val StatusMock = ElasticsearchStatusInfo("OK")
}
