package org.broadinstitute.clio.server.dataaccess

import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ElasticsearchIndex,
  ElasticsearchQueryMapper
}

import com.sksamuel.elastic4s.{HitReader, Indexable}

import scala.collection.mutable
import scala.concurrent.Future

class MemorySearchDAO extends MockSearchDAO {

  // TODO: We could probably be more specific than `Any` here
  val updateCalls: mutable.ArrayBuffer[(Any, Any, Any)] =
    mutable.ArrayBuffer.empty
  val queryCalls: mutable.ArrayBuffer[Any] = mutable.ArrayBuffer.empty

  override def updateMetadata[D: Indexable](
    id: String,
    document: D,
    index: ElasticsearchIndex[D]
  ): Future[Unit] = {
    updateCalls += ((id, document, index))
    super.updateMetadata(id, document, index)
  }

  override def queryMetadata[I, O, D: HitReader](
    input: I,
    index: ElasticsearchIndex[D],
    queryBuilder: ElasticsearchQueryMapper[I, O, D]
  ): Future[Seq[O]] = {
    queryCalls += input
    super.queryMetadata(input, index, queryBuilder)
  }
}
