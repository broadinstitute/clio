package org.broadinstitute.clio.server.dataaccess

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.sksamuel.elastic4s.searches.queries.{
  QueryDefinition,
  SimpleStringQueryDefinition
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
    updateCalls
      .flatMap(_._1)
      .find(update => {
        update.findAllByKey("entity_id").headOption.fold(false) { entityId =>
          entityId.toString().split('.').forall { keySegment =>
            queryDefinition
              .asInstanceOf[SimpleStringQueryDefinition]
              .query
              .contains(keySegment.replace("\"", ""))
          }
        }
      })
      .fold(
        super.queryMetadata(queryDefinition)
      )(Source.single)

  }

  override def getMostRecentDocument(
    implicit index: ElasticsearchIndex[_]
  ): Future[Option[Json]] = {
    Future.successful(updateCalls.lastOption.flatMap(_._1.lastOption))
  }
}
