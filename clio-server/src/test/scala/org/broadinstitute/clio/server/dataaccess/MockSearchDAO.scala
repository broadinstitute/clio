package org.broadinstitute.clio.server.dataaccess

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.sksamuel.elastic4s.searches.queries.QueryDefinition
import io.circe.Json
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex

import scala.collection.immutable
import scala.concurrent.Future

class MockSearchDAO extends SearchDAO {
  override def checkOk: Future[Unit] = Future.unit

  override def initialize(
    indexes: immutable.Seq[ElasticsearchIndex[_]]
  ): Future[Unit] = Future.unit

  override def close(): Future[Unit] = Future.unit

  override def updateMetadata(documents: Json*)(
    implicit index: ElasticsearchIndex[_]
  ): Future[Unit] = Future.unit

  override def queryMetadata(queryDefinition: QueryDefinition)(
    implicit index: ElasticsearchIndex[_]
  ): Source[Json, NotUsed] = {
    Source.empty[Json]
  }

  // Not implemented on purpose; there's nothing sensible to return here.
  override def getMostRecentDocument(
    implicit index: ElasticsearchIndex[_]
  ): Future[Option[Json]] = {
    Future.successful(None)
  }
}
