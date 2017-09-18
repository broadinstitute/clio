package org.broadinstitute.clio.server.dataaccess

import com.sksamuel.elastic4s.{HitReader, Indexable}
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ClioDocument,
  ElasticsearchIndex
}

import com.sksamuel.elastic4s.searches.queries.QueryDefinition

import scala.collection.mutable
import scala.concurrent.Future

class MemorySearchDAO extends MockSearchDAO {

  val updateCalls: mutable.ArrayBuffer[(ClioDocument, ElasticsearchIndex[_])] =
    mutable.ArrayBuffer.empty
  val queryCalls: mutable.ArrayBuffer[QueryDefinition] =
    mutable.ArrayBuffer.empty

  override def updateMetadata[D <: ClioDocument: Indexable](
    document: D,
    index: ElasticsearchIndex[D]
  ): Future[Unit] = {
    updateCalls += ((document, index))
    super.updateMetadata(document, index)
  }

  override def queryMetadata[D: HitReader](
    queryDefinition: QueryDefinition,
    index: ElasticsearchIndex[D]
  ): Future[Seq[D]] = {
    queryCalls += queryDefinition
    super.queryMetadata(queryDefinition, index)
  }
}
