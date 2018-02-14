package org.broadinstitute.clio.server.dataaccess

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.sksamuel.elastic4s.searches.queries.QueryDefinition
import io.circe.Json
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ClioDocument,
  ElasticsearchIndex
}

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

  override def queryMetadata[D <: ClioDocument](queryDefinition: QueryDefinition)(
    implicit index: ElasticsearchIndex[D]
  ): Source[D, NotUsed] = Source.failed(ex)

  override def updateMetadata(documents: Json*)(
    implicit index: ElasticsearchIndex[_]
  ): Future[Unit] = failure

  override def getMostRecentDocument(
    implicit index: ElasticsearchIndex[_]
  ): Future[Option[Json]] = failure
}
