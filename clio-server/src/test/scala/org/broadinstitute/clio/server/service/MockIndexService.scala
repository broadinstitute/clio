package org.broadinstitute.clio.server.service

import akka.NotUsed
import akka.stream.scaladsl.Source
import io.circe.Json
import io.circe.syntax._
import org.broadinstitute.clio.server.dataaccess.{
  MockPersistenceDAO,
  MockSearchDAO,
  PersistenceDAO,
  SearchDAO
}
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.transfer.model.TransferIndex
import org.broadinstitute.clio.util.model.UpsertId

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}

abstract class MockIndexService[
  TI <: TransferIndex
](
  persistenceDAO: PersistenceDAO = new MockPersistenceDAO(),
  searchDAO: SearchDAO = new MockSearchDAO(),
  elasticsearchIndex: ElasticsearchIndex[TI],
  override val transferIndex: TI
)(
  implicit
  executionContext: ExecutionContext
) extends IndexService(
      PersistenceService(persistenceDAO, searchDAO),
      SearchService(searchDAO),
      elasticsearchIndex,
      transferIndex
    ) {
  val queryCalls = ArrayBuffer.empty[transferIndex.QueryInputType]
  val queryAllCalls = ArrayBuffer.empty[transferIndex.QueryInputType]
  val upsertCalls = ArrayBuffer.empty[(transferIndex.KeyType, transferIndex.MetadataType)]

  import transferIndex.implicits._

  def emptyOutput: transferIndex.QueryOutputType

  override def upsertMetadata(
    transferKey: transferIndex.KeyType,
    transferMetadata: transferIndex.MetadataType
  ): Future[UpsertId] = {
    upsertCalls += ((transferKey, transferMetadata))
    Future.successful(UpsertId.nextId())
  }

  override def queryMetadata(
    transferInput: transferIndex.QueryInputType
  ): Source[Json, NotUsed] = {
    queryCalls += transferInput
    Source.single(emptyOutput.asJson)
  }

  override def queryAllMetadata(
    transferInput: transferIndex.QueryInputType
  ): Source[Json, NotUsed] = {
    queryAllCalls += transferInput
    Source.single(emptyOutput.asJson)
  }

}
