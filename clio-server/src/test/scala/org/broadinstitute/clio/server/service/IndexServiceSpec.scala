package org.broadinstitute.clio.server.service

import akka.stream.scaladsl.Sink
import io.circe.Json
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ClioDocument,
  ElasticsearchIndex
}
import org.broadinstitute.clio.server.{MockClioApp, TestKitSuite}
import org.broadinstitute.clio.server.dataaccess.{MemoryPersistenceDAO, MemorySearchDAO}
import org.broadinstitute.clio.transfer.model.TransferIndex
import org.broadinstitute.clio.util.model.{DocumentStatus, UpsertId}

abstract class IndexServiceSpec[
  TI <: TransferIndex,
  D <: ClioDocument: ElasticsearchIndex
](specificService: String)
    extends TestKitSuite(specificService + "Spec") {

  val memoryPersistenceDAO = new MemoryPersistenceDAO()
  val memorySearchDAO = new MemorySearchDAO()

  val indexService: IndexService[TI, D] = {
    val app = MockClioApp(
      searchDAO = memorySearchDAO,
      persistenceDAO = memoryPersistenceDAO
    )
    val searchService = SearchService(app)
    val persistenceService = PersistenceService(app)
    getService(persistenceService, searchService)
  }

  def index: ElasticsearchIndex[D]
  def dummyKey: indexService.transferIndex.KeyType
  def dummyInput: indexService.transferIndex.QueryInputType

  def getDummyMetadata(
    documentStatus: Option[DocumentStatus]
  ): indexService.transferIndex.MetadataType

  def getService(
    persistenceService: PersistenceService,
    searchService: SearchService
  ): IndexService[TI, D]

  def copyDocumentWithUpsertId(originalDocument: D, upsertId: UpsertId): D
  def documentToJson(document: D): Json

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
      val expectedDocument = copyDocumentWithUpsertId(
        indexService.v1DocumentConverter.withMetadata(
          indexService.v1DocumentConverter.empty(dummyKey),
          transferMetadata.withDocumentStatus(expectedDocumentStatus)
        ),
        returnedUpsertId
      )

      memoryPersistenceDAO.writeCalls should be(Seq((expectedDocument, index)))
      memorySearchDAO.updateCalls should be(
        Seq((Seq(documentToJson(expectedDocument)), index))
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
