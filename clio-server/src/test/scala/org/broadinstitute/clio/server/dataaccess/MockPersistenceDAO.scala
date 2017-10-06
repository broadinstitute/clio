package org.broadinstitute.clio.server.dataaccess
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ClioDocument,
  ElasticsearchIndex
}

import com.sksamuel.elastic4s.Indexable

import scala.concurrent.{ExecutionContext, Future}

import java.nio.file.{Path, Paths}

class MockPersistenceDAO extends PersistenceDAO {
  override def rootPath: Path = Paths.get("/")

  override def initialize(
    indexes: Seq[ElasticsearchIndex[_]]
  )(implicit ec: ExecutionContext): Future[Unit] =
    Future.successful(())

  override def writeUpdate[D <: ClioDocument](
    document: D,
    index: ElasticsearchIndex[D]
  )(implicit ec: ExecutionContext, indexable: Indexable[D]): Future[Unit] =
    Future.successful(())
}
