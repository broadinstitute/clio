package org.broadinstitute.clio.server.dataaccess

import com.sksamuel.elastic4s.{HitReader, Indexable}
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ClioDocument,
  ElasticsearchIndex
}

import com.sksamuel.elastic4s.searches.queries.QueryDefinition

import scala.concurrent.Future

class FailingSearchDAO extends SearchDAO {
  val failure = Future.failed(new Exception("Tried to use failing search DAO"))

  override def checkOk: Future[Unit] = failure

  override def initialize(): Future[Unit] = failure

  override def close(): Future[Unit] = failure

  override def queryMetadata[D: HitReader](
    queryDefinition: QueryDefinition,
    index: ElasticsearchIndex[D]
  ): Future[Seq[D]] = failure

  override def updateMetadata[D <: ClioDocument: Indexable](
    document: D,
    index: ElasticsearchIndex[D]
  ): Future[Unit] = failure
}
