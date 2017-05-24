package org.broadinstitute.clio.dataaccess
import org.broadinstitute.clio.model.ElasticsearchStatusInfo

import scala.concurrent.Future
import scala.concurrent.duration._

class MockElasticsearchDAO extends ElasticsearchDAO {
  override def getClusterStatus: Future[ElasticsearchStatusInfo] = Future.successful(MockElasticsearchDAO.StatusMock)

  override def isReady: Future[Boolean] = Future.successful(true)

  override val readyRetries: Int = 0

  override val readyPatience: FiniteDuration = 0.seconds

  override def close(): Future[Unit] = Future.successful(())
}

object MockElasticsearchDAO {
  val StatusMock = ElasticsearchStatusInfo("Mock Elasticsearch Status", 123, 456)
}
