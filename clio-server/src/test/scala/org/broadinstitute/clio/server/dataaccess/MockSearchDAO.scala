package org.broadinstitute.clio.server.dataaccess

import com.sksamuel.elastic4s.{HitReader, Indexable}
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ClioDocument,
  ElasticsearchIndex
}

import com.sksamuel.elastic4s.searches.queries.QueryDefinition

import scala.concurrent.Future

class MockSearchDAO extends SearchDAO {
  override def checkOk: Future[Unit] = {
    Future.successful(())
  }

  override def initialize(): Future[Unit] = {
    Future.successful(())
  }

  override def close(): Future[Unit] = {
    Future.successful(())
  }

  override def updateMetadata[D <: ClioDocument: Indexable](
    document: D,
    index: ElasticsearchIndex[D]
  ): Future[Unit] = {
    Future.successful(())
  }

  override def queryMetadata[D: HitReader](
    queryDefinition: QueryDefinition,
    index: ElasticsearchIndex[D]
  ): Future[Seq[D]] = {
    Future.successful(Seq.empty[D])
  }
}
