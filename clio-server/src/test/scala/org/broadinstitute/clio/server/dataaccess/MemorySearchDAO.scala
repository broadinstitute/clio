package org.broadinstitute.clio.server.dataaccess

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.sksamuel.elastic4s.searches.queries.QueryDefinition
import io.circe.Json
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.transfer.model.TransferIndex

import scala.collection.mutable
import scala.concurrent.Future

class MemorySearchDAO extends MockSearchDAO {

  val updateCalls: mutable.ArrayBuffer[(Seq[Json], ElasticsearchIndex[TransferIndex])] =
    mutable.ArrayBuffer.empty

  val queryCalls: mutable.ArrayBuffer[QueryDefinition] =
    mutable.ArrayBuffer.empty

  override def updateMetadata(documents: Json*)(
    implicit index: ElasticsearchIndex[_]
  ): Future[Unit] = {
    updateCalls += ((documents, index.asInstanceOf[ElasticsearchIndex[TransferIndex]]))
    super.updateMetadata(documents: _*)
  }

  override def queryMetadata(queryDefinition: QueryDefinition)(
    implicit
    index: ElasticsearchIndex[_]
  ): Source[Json, NotUsed] = {
    queryCalls += queryDefinition
    super.queryMetadata(queryDefinition)
  }

  override def getMostRecentDocument(
    implicit index: ElasticsearchIndex[_]
  ): Future[Option[Json]] = {
    Future.successful(updateCalls.lastOption.flatMap(_._1.lastOption))
  }
}
