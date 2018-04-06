package org.broadinstitute.clio.server.service

import akka.NotUsed
import akka.stream.scaladsl.Source
import io.circe.Json
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.server.dataaccess.{
  MockPersistenceDAO,
  MockSearchDAO,
  PersistenceDAO,
  SearchDAO
}
import org.broadinstitute.clio.transfer.model.ClioIndex
import org.broadinstitute.clio.util.model.UpsertId

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext

abstract class MockIndexService[
  CI <: ClioIndex
](
  persistenceDAO: PersistenceDAO = new MockPersistenceDAO(),
  searchDAO: SearchDAO = new MockSearchDAO(),
  elasticsearchIndex: ElasticsearchIndex[CI],
  override val clioIndex: CI
)(
  implicit
  executionContext: ExecutionContext
) extends IndexService(
      new PersistenceService(persistenceDAO, searchDAO),
      new SearchService(searchDAO),
      elasticsearchIndex,
      clioIndex
    ) {

  val queryCalls: ArrayBuffer[clioIndex.QueryInputType] =
    ArrayBuffer.empty[clioIndex.QueryInputType]

  val queryAllCalls: ArrayBuffer[clioIndex.QueryInputType] =
    ArrayBuffer.empty[clioIndex.QueryInputType]

  val upsertCalls: ArrayBuffer[(clioIndex.KeyType, clioIndex.MetadataType)] =
    ArrayBuffer.empty[(clioIndex.KeyType, clioIndex.MetadataType)]

  def emptyOutput: Json

  override def upsertMetadata(
    key: clioIndex.KeyType,
    metadata: clioIndex.MetadataType,
    force: Boolean = false
  ): Source[UpsertId, NotUsed] = {
    upsertCalls += ((key, metadata))
    Source.single(UpsertId.nextId())
  }

  override def queryMetadata(
    input: clioIndex.QueryInputType
  ): Source[Json, NotUsed] = {
    queryCalls += input
    Source.single(emptyOutput)
  }

  override def queryAllMetadata(
    input: clioIndex.QueryInputType
  ): Source[Json, NotUsed] = {
    queryAllCalls += input
    Source.single(emptyOutput)
  }

}
