package org.broadinstitute.clio.server.dataaccess
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ElasticsearchIndex,
  ElasticsearchQueryMapper
}

import com.sksamuel.elastic4s.{HitReader, Indexable}

import scala.concurrent.Future

class FailingSearchDAO extends SearchDAO {
  val failure = Future.failed(new Exception("Tried to use failing search DAO"))

  override def checkOk: Future[Unit] = failure

  override def initialize(): Future[Unit] = failure

  override def close(): Future[Unit] = failure

  override def queryMetadata[I, O, D: HitReader](
    input: I,
    index: ElasticsearchIndex[D],
    queryMapper: ElasticsearchQueryMapper[I, O, D]
  ): Future[Seq[O]] = failure

  override def updateMetadata[D: Indexable](
    id: String,
    document: D,
    index: ElasticsearchIndex[D]
  ): Future[Unit] = failure
}
