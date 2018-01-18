package org.broadinstitute.clio.server.dataaccess

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.sksamuel.elastic4s.searches.queries.QueryDefinition
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ClioDocument,
  ElasticsearchIndex
}

import scala.collection.mutable
import scala.concurrent.Future

class MemorySearchDAO extends MockSearchDAO {

  val updateCalls: mutable.ArrayBuffer[(ClioDocument, ElasticsearchIndex[_])] =
    mutable.ArrayBuffer.empty

  val queryCalls: mutable.ArrayBuffer[QueryDefinition] =
    mutable.ArrayBuffer.empty

  override def updateMetadata[D <: ClioDocument](document: D)(
    implicit index: ElasticsearchIndex[D]
  ): Future[Unit] = {
    updateCalls += ((document, index))
    super.updateMetadata(document)
  }

  override def queryMetadata[D](queryDefinition: QueryDefinition)(
    implicit
    index: ElasticsearchIndex[D]
  ): Source[D, NotUsed] = {
    queryCalls += queryDefinition
    super.queryMetadata(queryDefinition)
  }

  override def getMostRecentDocument[D <: ClioDocument](
    implicit index: ElasticsearchIndex[D]
  ): Future[Option[D]] = {
    Future.successful(updateCalls.lastOption.map(_._1.asInstanceOf[D]))
  }
}
