package org.broadinstitute.clio.server.service

import akka.stream.scaladsl.Sink
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ElasticsearchIndex,
  ElasticsearchUtil
}
import org.broadinstitute.clio.server.TestKitSuite
import org.broadinstitute.clio.server.dataaccess.{MemoryPersistenceDAO, MemorySearchDAO}
import org.broadinstitute.clio.transfer.model.TransferIndex
import org.broadinstitute.clio.util.model.DocumentStatus
import io.circe.syntax._
import org.broadinstitute.clio.util.json.ModelAutoDerivation

abstract class IndexServiceSpec[
  TI <: TransferIndex
](specificService: String)
    extends TestKitSuite(specificService + "Spec")
    with ModelAutoDerivation {

  val memoryPersistenceDAO = new MemoryPersistenceDAO()
  val memorySearchDAO = new MemorySearchDAO()

  val indexService: IndexService[TI] = {

    val searchService = new SearchService(memorySearchDAO)
    val persistenceService = new PersistenceService(memoryPersistenceDAO, memorySearchDAO)
    getService(persistenceService, searchService)
  }

  def elasticsearchIndex: ElasticsearchIndex[TI]
  def dummyKey: indexService.transferIndex.KeyType
  def dummyInput: indexService.transferIndex.QueryInputType

  def getDummyMetadata(
    documentStatus: Option[DocumentStatus]
  ): indexService.transferIndex.MetadataType

  def getService(
    persistenceService: PersistenceService,
    searchService: SearchService
  ): IndexService[TI]

  behavior of specificService

  it should "upsertMetadata" in {
    clearMemory()
    upsertMetadataTest(None)
  }

  it should "upsertMetadata with document_status explicitly set to Normal" in {
    clearMemory()
    upsertMetadataTest(
      Option(DocumentStatus.Normal)
    )
  }

  it should "upsertMetadata with document_status explicitly set to Deleted" in {
    clearMemory()
    upsertMetadataTest(
      Option(DocumentStatus.Deleted)
    )
  }

  it should "queryData" in {
    clearMemory()
    for {
      _ <- indexService.queryMetadata(dummyInput).runWith(Sink.seq)
    } yield {
      memorySearchDAO.updateCalls should be(empty)
      memorySearchDAO.queryCalls should be(
        Seq(
          indexService.v1QueryConverter.buildQuery(
            dummyInput.withDocumentStatus(Option(DocumentStatus.Normal))
          )(
            elasticsearchIndex
          )
        )
      )
    }
  }

  private def upsertMetadataTest(
    documentStatus: Option[DocumentStatus]
  ) = {
    val transferMetadata = getDummyMetadata(documentStatus)
    val expectedDocumentStatus = Option(documentStatus.getOrElse(DocumentStatus.Normal))
    for {
      returnedUpsertId <- indexService.upsertMetadata(
        dummyKey,
        transferMetadata
      )
    } yield {
      val expectedDocument =
        indexService.v1DocumentConverter
          .document(
            dummyKey,
            transferMetadata.withDocumentStatus(expectedDocumentStatus)
          )
          .deepMerge(
            Map(
              ElasticsearchUtil
                .toElasticsearchName(ElasticsearchIndex.UpsertIdElasticsearchName) -> returnedUpsertId
            ).asJson
          )

      memoryPersistenceDAO.writeCalls should be(
        Seq((expectedDocument, elasticsearchIndex))
      )
      memorySearchDAO.updateCalls should be(
        Seq((Seq(expectedDocument), elasticsearchIndex))
      )
      memorySearchDAO.queryCalls should be(empty)
    }
  }

  private def clearMemory(): Unit = {
    memoryPersistenceDAO.writeCalls.clear()
    memorySearchDAO.updateCalls.clear()
    memorySearchDAO.queryCalls.clear()
  }
}
