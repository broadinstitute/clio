package org.broadinstitute.clio.server.dataaccess

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.sksamuel.elastic4s.searches.queries.QueryDefinition
import io.circe.Json
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ClioDocument,
  ElasticsearchIndex
}

import scala.collection.mutable
import scala.concurrent.Future

class MemorySearchDAO extends MockSearchDAO {

  val updateCalls: mutable.ArrayBuffer[(Json, ElasticsearchIndex[_])] =
    mutable.ArrayBuffer.empty

  val queryCalls: mutable.ArrayBuffer[QueryDefinition] =
    mutable.ArrayBuffer.empty

  override def updateMetadata(document: Json)(
    implicit index: ElasticsearchIndex[_]
  ): Future[Unit] = {
    updateCalls += ((document, index))
    super.updateMetadata(document)
  }

  override def queryMetadata[D <: ClioDocument](queryDefinition: QueryDefinition)(
    implicit
    index: ElasticsearchIndex[D]
  ): Source[D, NotUsed] = {
    queryCalls += queryDefinition
    super.queryMetadata[D](queryDefinition)
  }

  override def getMostRecentDocument(
    implicit index: ElasticsearchIndex[_]
  ): Future[Option[Json]] = {
    Future.successful(updateCalls.lastOption.map(_._1))
  }
}
