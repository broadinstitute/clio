package org.broadinstitute.clio.dataaccess
import org.broadinstitute.clio.model.{
  ElasticsearchIndex,
  ElasticsearchStatusInfo
}

import scala.concurrent.Future
import scala.concurrent.duration._

class MockElasticsearchDAO extends ElasticsearchDAO {
  override def getClusterStatus: Future[ElasticsearchStatusInfo] =
    Future.successful(MockElasticsearchDAO.StatusMock)

  override def isReady: Future[Boolean] = Future.successful(true)

  override val readyRetries: Int = 0

  override val readyPatience: FiniteDuration = 0.seconds

  override def existsIndexType(index: ElasticsearchIndex): Future[Boolean] =
    Future.successful(true)

  override def createIndexType(index: ElasticsearchIndex,
                               replicate: Boolean): Future[Unit] =
    Future.successful(())

  override def updateFieldDefinitions(
    index: ElasticsearchIndex
  ): Future[Unit] = Future.successful(())

  override def close(): Future[Unit] = Future.successful(())
}

object MockElasticsearchDAO {
  val StatusMock =
    ElasticsearchStatusInfo("Mock Elasticsearch Status", 123, 456)
}
