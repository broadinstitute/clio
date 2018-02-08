package org.broadinstitute.clio.server.dataaccess

import java.nio.file.{Path, Paths}

import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ClioDocument,
  ElasticsearchIndex
}

import scala.concurrent.{ExecutionContext, Future}

class MockPersistenceDAO extends PersistenceDAO(recoveryParallelism = 1) {
  override def rootPath: Path = Paths.get("/")

  override def initialize(
    indexes: Seq[ElasticsearchIndex[_]],
    version: String
  )(implicit ec: ExecutionContext): Future[Unit] =
    Future.successful(())

  override def writeUpdate[D <: ClioDocument](document: D)(
    implicit ec: ExecutionContext,
    index: ElasticsearchIndex[D]
  ): Future[Unit] =
    Future.successful(())
}
