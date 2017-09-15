package org.broadinstitute.clio.server.dataaccess

import com.sksamuel.elastic4s.searches.queries.QueryDefinition
import com.sksamuel.elastic4s.{HitReader, Indexable}
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex

import scala.collection.mutable
import scala.concurrent.Future

class MemorySearchDAO extends MockSearchDAO {

  // TODO: We could probably be more specific than `Any` here
  val updateCalls: mutable.ArrayBuffer[(Any, Any, Any)] =
    mutable.ArrayBuffer.empty
  val queryCalls: mutable.ArrayBuffer[QueryDefinition] =
    mutable.ArrayBuffer.empty

  override def updateMetadata[D: Indexable](
    id: String,
    document: D,
    index: ElasticsearchIndex[D]
  ): Future[Unit] = {
    updateCalls += ((id, document, index))
    super.updateMetadata(id, document, index)
  }

  override def queryMetadata[D: HitReader](
    queryDefinition: QueryDefinition,
    index: ElasticsearchIndex[D]
  ): Future[Seq[D]] = {
    queryCalls += queryDefinition
    super.queryMetadata(queryDefinition, index)
  }
}
