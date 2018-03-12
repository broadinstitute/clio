package org.broadinstitute.clio.server.service

import akka.NotUsed
import akka.stream.scaladsl.Source
import org.broadinstitute.clio.server.dataaccess.{
  MockPersistenceDAO,
  MockSearchDAO,
  PersistenceDAO,
  SearchDAO
}
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ClioDocument,
  ElasticsearchIndex
}
import org.broadinstitute.clio.transfer.model.TransferIndex
import org.broadinstitute.clio.util.model.UpsertId

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

abstract class MockIndexService[
  TI <: TransferIndex,
  D <: ClioDocument: ElasticsearchIndex
](
  persistenceDAO: PersistenceDAO = new MockPersistenceDAO(),
  searchDAO: SearchDAO = new MockSearchDAO(),
  override val transferIndex: TI
)(
  implicit
  executionContext: ExecutionContext,
  documentTag: ClassTag[D]
) extends IndexService(
      PersistenceService(persistenceDAO, searchDAO),
      SearchService(searchDAO),
      transferIndex
    ) {
  val queryCalls = ArrayBuffer.empty[transferIndex.QueryInputType]
  val queryAllCalls = ArrayBuffer.empty[transferIndex.QueryInputType]
  val upsertCalls = ArrayBuffer.empty[(transferIndex.KeyType, transferIndex.MetadataType)]

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
  ): Source[transferIndex.QueryOutputType, NotUsed] = {
    queryCalls += transferInput
    Source.single(emptyOutput)
  }

  override def queryAllMetadata(
    transferInput: transferIndex.QueryInputType
  ): Source[transferIndex.QueryOutputType, NotUsed] = {
    queryAllCalls += transferInput
    Source.single(emptyOutput)
  }

}
