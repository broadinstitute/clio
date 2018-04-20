package org.broadinstitute.clio.server.dataaccess

import akka.NotUsed
import akka.stream.scaladsl.Source
import io.circe.syntax._
import io.circe.{Json, JsonObject}
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.transfer.model.ClioIndex

import scala.collection.mutable
import scala.concurrent.Future

class MemorySearchDAO extends MockSearchDAO {

  val updateCalls: mutable.ArrayBuffer[(Seq[Json], ElasticsearchIndex[ClioIndex])] =
    mutable.ArrayBuffer.empty

  val queryCalls: mutable.ArrayBuffer[JsonObject] =
    mutable.ArrayBuffer.empty

  override def updateMetadata(documents: Seq[Json])(
    implicit index: ElasticsearchIndex[_]
  ): Future[Unit] = {
    updateCalls += ((documents, index.asInstanceOf[ElasticsearchIndex[ClioIndex]]))
    super.updateMetadata(documents)
  }

  override def updateMetadata(document: Json)(
    implicit index: ElasticsearchIndex[_]
  ): Future[Unit] = {
    updateMetadata(Seq(document))
  }

  override def rawQuery(json: JsonObject)(
    implicit
    index: ElasticsearchIndex[_]
  ): Source[Json, NotUsed] = {
    queryCalls += json
    Source.single(json.asJson)
  }

  override def getMostRecentDocument(
    implicit index: ElasticsearchIndex[_]
  ): Future[Option[Json]] = {
    Future.successful(updateCalls.lastOption.flatMap(_._1.lastOption))
  }
}
