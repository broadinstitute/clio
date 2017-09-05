package org.broadinstitute.clio.server.dataaccess
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ClioDocument,
  ElasticsearchIndex
}

import com.sksamuel.elastic4s.Indexable

import scala.concurrent.{ExecutionContext, Future}

import java.nio.file.Path

class FailingPersistenceDAO extends PersistenceDAO {
  val ex = new Exception("Tried to use failing persistence DAO")

  override def rootPath: Path = throw ex
  override def checkRoot(): Unit = throw ex

  override def writeUpdate[D <: ClioDocument](
    document: D,
    index: ElasticsearchIndex[D]
  )(implicit ec: ExecutionContext, indexable: Indexable[D]): Future[Unit] =
    Future.failed(ex)
}
