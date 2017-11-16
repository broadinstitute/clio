package org.broadinstitute.clio.server.dataaccess

import java.nio.file.{Path, Paths}

import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ClioDocument,
  ElasticsearchIndex
}

import scala.concurrent.{ExecutionContext, Future}

class MockPersistenceDAO extends PersistenceDAO {
  override def rootPath: Path = Paths.get("/")

  override def initialize(
    indexes: Seq[ElasticsearchIndex[_]]
  )(implicit ec: ExecutionContext): Future[Unit] =
    Future.successful(())

  override def writeUpdate[D <: ClioDocument](
    document: D,
    index: ElasticsearchIndex[D]
  )(implicit ec: ExecutionContext): Future[Unit] =
    Future.successful(())
}
