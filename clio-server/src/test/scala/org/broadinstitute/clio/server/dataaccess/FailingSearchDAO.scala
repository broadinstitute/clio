package org.broadinstitute.clio.server.dataaccess

import com.sksamuel.elastic4s.searches.queries.QueryDefinition
import com.sksamuel.elastic4s.{HitReader, Indexable}
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ClioDocument,
  ElasticsearchIndex
}

import scala.collection.immutable
import scala.concurrent.Future

class FailingSearchDAO extends SearchDAO {
  val failure = Future.failed(new Exception("Tried to use failing search DAO"))

  override def checkOk: Future[Unit] = failure

  override def initialize(
    indexes: immutable.Seq[ElasticsearchIndex[_]]
  ): Future[Unit] = failure

  override def close(): Future[Unit] = failure

  override def queryMetadata[D <: ClioDocument: HitReader](
    queryDefinition: QueryDefinition,
    index: ElasticsearchIndex[D]
  ): Future[Seq[D]] = failure

  override def updateMetadata[D <: ClioDocument: Indexable](
    document: D,
    index: ElasticsearchIndex[D]
  ): Future[Unit] = failure

  override def getMostRecentDocument[D <: ClioDocument: HitReader](
    index: ElasticsearchIndex[D]
  ): Future[Option[D]] = failure
}
