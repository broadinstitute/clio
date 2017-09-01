package org.broadinstitute.clio.server.dataaccess

import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ElasticsearchIndex,
  ElasticsearchQueryMapper
}

import com.sksamuel.elastic4s.{HitReader, Indexable}

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

  override def updateMetadata[D: Indexable](
    id: String,
    document: D,
    index: ElasticsearchIndex[D]
  ): Future[Unit] = {
    Future.successful(())
  }

  override def queryMetadata[I, O, D: HitReader](
    queryInput: I,
    index: ElasticsearchIndex[D],
    queryBuilder: ElasticsearchQueryMapper[I, O, D]
  ): Future[Seq[O]] = {
    Future.successful(Seq.empty[O])
  }
}
