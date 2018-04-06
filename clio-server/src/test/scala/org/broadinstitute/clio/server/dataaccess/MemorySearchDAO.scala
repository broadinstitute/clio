package org.broadinstitute.clio.server.dataaccess

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.sksamuel.elastic4s.searches.queries.{
  BoolQueryDefinition,
  QueryDefinition,
  QueryStringQueryDefinition
}
import io.circe.Json
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.transfer.model.ClioIndex

import scala.collection.mutable
import scala.concurrent.Future

class MemorySearchDAO extends MockSearchDAO {

  val updateCalls: mutable.ArrayBuffer[(Seq[Json], ElasticsearchIndex[ClioIndex])] =
    mutable.ArrayBuffer.empty

  val queryCalls: mutable.ArrayBuffer[QueryDefinition] =
    mutable.ArrayBuffer.empty

  override def updateMetadata(documents: Json*)(
    implicit index: ElasticsearchIndex[_]
  ): Future[Unit] = {
    updateCalls += ((documents, index.asInstanceOf[ElasticsearchIndex[ClioIndex]]))
    super.updateMetadata(documents: _*)
  }

  override def queryMetadata(queryDefinition: QueryDefinition)(
    implicit
    index: ElasticsearchIndex[_]
  ): Source[Json, NotUsed] = {
    queryCalls += queryDefinition
    queryDefinition match {
      case boolQuery: BoolQueryDefinition =>
        val entityId = boolQuery.must.map {
          case innerQuery: QueryStringQueryDefinition =>
            innerQuery.query.replace("\"", "")
          case _ => Source.empty
        }.mkString(".")
        updateCalls
          .flatMap(_._1)
          .find(update => {
            update
              .findAllByKey("entity_id")
              .forall(
                updateEntityId =>
                  updateEntityId.toString().split(".").deep == entityId.toString
                    .split(".")
                    .deep
              )
          })
          .fold(
            super.queryMetadata(queryDefinition)
          )(Source.single)
      case _ => Source.empty
    }
  }

  override def getMostRecentDocument(
    implicit index: ElasticsearchIndex[_]
  ): Future[Option[Json]] = {
    Future.successful(updateCalls.lastOption.flatMap(_._1.lastOption))
  }
}
