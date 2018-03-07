package org.broadinstitute.clio.server.service

import akka.NotUsed
import akka.stream.scaladsl.Source
import org.broadinstitute.clio.server.{ClioApp, MockClioApp}
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{ClioDocument, ElasticsearchIndex}
import org.broadinstitute.clio.transfer.model.TransferIndex
import org.broadinstitute.clio.util.model.UpsertId

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}

abstract class MockIndexService[TI <: TransferIndex, D <: ClioDocument: ElasticsearchIndex](
  app: ClioApp = MockClioApp(),
  override val transferIndex: TI,
  elasticsearchIndex: ElasticsearchIndex[D]
)(
  implicit
  executionContext: ExecutionContext
) extends IndexService(
  PersistenceService(app),
  SearchService(app),
  transferIndex,
  elasticsearchIndex
) {
  val queryCalls: ArrayBuffer[transferIndex.QueryInputType]
  val queryAllCalls: ArrayBuffer[transferIndex.QueryInputType]
  val upsertCalls: ArrayBuffer[(transferIndex.KeyType, transferIndex.MetadataType)]

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
