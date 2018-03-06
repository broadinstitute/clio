package org.broadinstitute.clio.server.dataaccess

import java.time.OffsetDateTime

import better.files.File
import io.circe.Json
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex

import scala.concurrent.{ExecutionContext, Future}

class FailingPersistenceDAO extends PersistenceDAO {
  val ex = new Exception("Tried to use failing persistence DAO")

  override def rootPath: File = throw ex

  override def writeUpdate(
    document: Json,
    index: ElasticsearchIndex[_],
    dt: OffsetDateTime
  )(
    implicit ec: ExecutionContext
  ): Future[Unit] = Future.failed(ex)
}
