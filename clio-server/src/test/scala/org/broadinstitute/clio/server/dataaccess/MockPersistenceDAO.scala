package org.broadinstitute.clio.server.dataaccess

import java.time.OffsetDateTime

import better.files.File
import io.circe.Json
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex

import scala.concurrent.{ExecutionContext, Future}

class MockPersistenceDAO extends PersistenceDAO {
  override def rootPath: File = File.root

  override def initialize(
    indexes: Seq[ElasticsearchIndex[_]],
    version: String
  )(implicit ec: ExecutionContext): Future[Unit] = Future.unit

  override def writeUpdate(document: Json, index: ElasticsearchIndex[_], dt: OffsetDateTime)(
    implicit ec: ExecutionContext
  ): Future[Unit] = Future.unit
}
