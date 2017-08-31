package org.broadinstitute.clio.server.dataaccess

import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ElasticsearchIndex,
  ElasticsearchQueryMapper
}

import com.sksamuel.elastic4s.{HitReader, Indexable}

import scala.concurrent.Future

class MemorySearchDAO extends MockSearchDAO {

  var updateCalls: Seq[(_, _, _)] = Seq.empty
  var queryCalls: Seq[_] = Seq.empty

  override def updateMetadata[D: Indexable](
    id: String,
    document: D,
    index: ElasticsearchIndex[D]
  ): Future[Unit] = {
    updateCalls :+= ((id, document, index))
    super.updateMetadata(id, document, index)
  }

  override def queryMetadata[I, O, D: HitReader](
    input: I,
    index: ElasticsearchIndex[D],
    queryBuilder: ElasticsearchQueryMapper[I, O, D]
  ): Future[Seq[O]] = {
    queryCalls :+= input
    super.queryMetadata(input, index, queryBuilder)
  }
}
