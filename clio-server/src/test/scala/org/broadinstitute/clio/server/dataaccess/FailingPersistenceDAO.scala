package org.broadinstitute.clio.server.dataaccess

import java.nio.file.Path

import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ClioDocument,
  ElasticsearchIndex
}

import scala.concurrent.{ExecutionContext, Future}

class FailingPersistenceDAO extends PersistenceDAO(recoveryParallelism = 1) {
  val ex = new Exception("Tried to use failing persistence DAO")

  override def rootPath: Path = throw ex

  override def writeUpdate[D <: ClioDocument](document: D)(
    implicit ec: ExecutionContext,
    index: ElasticsearchIndex[D]
  ): Future[Unit] = Future.failed(ex)
}
