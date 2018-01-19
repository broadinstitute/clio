package org.broadinstitute.clio.server.dataaccess

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.sksamuel.elastic4s.searches.queries.QueryDefinition
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ClioDocument,
  ElasticsearchIndex
}

import scala.collection.immutable
import scala.concurrent.Future

class MockSearchDAO extends SearchDAO {
  override def checkOk: Future[Unit] = {
    Future.successful(())
  }

  override def initialize(
    indexes: immutable.Seq[ElasticsearchIndex[_]]
  ): Future[Unit] = {
    Future.successful(())
  }

  override def close(): Future[Unit] = {
    Future.successful(())
  }

  override def updateMetadata[D <: ClioDocument](document: D)(
    implicit index: ElasticsearchIndex[D]
  ): Future[Unit] = {
    Future.successful(())
  }

  override def queryMetadata[D](queryDefinition: QueryDefinition)(
    implicit index: ElasticsearchIndex[D]
  ): Source[D, NotUsed] = {
    Source.empty[D]
  }

  // Not implemented on purpose; there's nothing sensible to return here.
  override def getMostRecentDocument[D <: ClioDocument](
    implicit index: ElasticsearchIndex[D]
  ): Future[Option[D]] = {
    Future.successful(None)
  }
}
