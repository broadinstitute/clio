package org.broadinstitute.clio.server.dataaccess

import akka.NotUsed
import akka.stream.scaladsl.Source
import io.circe.{Json, JsonObject}
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex

import scala.collection.immutable
import scala.concurrent.Future

class FailingSearchDAO extends SearchDAO {
  val ex: Throwable = new Exception("Tried to use failing search DAO")
  val failure: Future[Nothing] = Future.failed(ex)

  override def checkOk: Future[Unit] = failure

  override def initialize(
    indexes: immutable.Seq[ElasticsearchIndex[_]]
  ): Future[Unit] = failure

  override def close(): Future[Unit] = failure

  override def rawQuery(json: JsonObject)(
    implicit index: ElasticsearchIndex[_]
  ): Source[Json, NotUsed] = Source.failed(ex)

  override def updateMetadata(documents: Seq[Json])(
    implicit index: ElasticsearchIndex[_]
  ): Future[Unit] = failure

  override def updateMetadata(document: Json)(
    implicit index: ElasticsearchIndex[_]
  ): Future[Unit] = failure

  override def getMostRecentDocument(
    implicit index: ElasticsearchIndex[_]
  ): Future[Option[Json]] = failure
}
