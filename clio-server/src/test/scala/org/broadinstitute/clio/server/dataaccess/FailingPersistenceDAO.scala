package org.broadinstitute.clio.server.dataaccess

import java.time.OffsetDateTime

import better.files.File
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ClioDocument,
  ElasticsearchIndex
}

import scala.concurrent.{ExecutionContext, Future}

class FailingPersistenceDAO extends PersistenceDAO {
  val ex = new Exception("Tried to use failing persistence DAO")

  override def rootPath: File = throw ex

  override def writeUpdate[D <: ClioDocument](document: D, dt: OffsetDateTime)(
    implicit ec: ExecutionContext,
    index: ElasticsearchIndex[D]
  ): Future[Unit] = Future.failed(ex)
}
