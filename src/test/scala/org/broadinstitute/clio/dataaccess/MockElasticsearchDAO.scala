package org.broadinstitute.clio.dataaccess
import org.broadinstitute.clio.model.ElasticsearchStatusInfo

import scala.concurrent.Future

class MockElasticsearchDAO extends ElasticsearchDAO {
  override def getClusterStatus: Future[ElasticsearchStatusInfo] = Future.successful(MockElasticsearchDAO.StatusMock)

  override def close(): Future[Unit] = Future.successful(())
}

object MockElasticsearchDAO {
  val StatusMock = ElasticsearchStatusInfo("Mock Elasticsearch Status", 123, 456)
}
