package org.broadinstitute.clio.server.service

import akka.stream.scaladsl.Sink
import org.broadinstitute.clio.server.{MockClioApp, TestKitSuite}
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.server.dataaccess.{MemoryPersistenceDAO, MemorySearchDAO}
import org.broadinstitute.clio.transfer.model.wgsubam.{
  TransferWgsUbamV1Key,
  TransferWgsUbamV1Metadata,
  TransferWgsUbamV1QueryInput
}
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

class WgsUbamServiceSpec extends TestKitSuite("WgsUbamServiceSpec") {
  behavior of "WgsUbamService"

  it should "upsertMetadata" in {
    upsertMetadataTest(None, Option(DocumentStatus.Normal))
  }

  it should "upsertMetadata with document_status explicitly set to Normal" in {
    upsertMetadataTest(
      Option(DocumentStatus.Normal),
      Option(DocumentStatus.Normal)
    )
  }

  it should "upsertMetadata with document_status explicitly set to Deleted" in {
    upsertMetadataTest(
      Option(DocumentStatus.Deleted),
      Option(DocumentStatus.Deleted)
    )
  }

  it should "queryData" in {
    val memorySearchDAO = new MemorySearchDAO()
    val app = MockClioApp(searchDAO = memorySearchDAO)
    val searchService = SearchService(app)
    val persistenceService = PersistenceService(app)
    val wgsUbamService = new WgsUbamService(persistenceService, searchService)

    val transferInput =
      TransferWgsUbamV1QueryInput(project = Option("testProject"))
    for {
      _ <- wgsUbamService.queryMetadata(transferInput).runWith(Sink.seq)
    } yield {
      memorySearchDAO.updateCalls should be(empty)
      memorySearchDAO.queryCalls should be(
        Seq(
          WgsUbamService.v1QueryConverter.buildQuery(
            transferInput.copy(documentStatus = Option(DocumentStatus.Normal))
          )
        )
      )
    }
  }

  private def upsertMetadataTest(
    documentStatus: Option[DocumentStatus],
    expectedDocumentStatus: Option[DocumentStatus]
  ) = {
    val memoryPersistenceDAO = new MemoryPersistenceDAO()
    val memorySearchDAO = new MemorySearchDAO()
    val app = MockClioApp(
      searchDAO = memorySearchDAO,
      persistenceDAO = memoryPersistenceDAO
    )
    val searchService = SearchService(app)
    val persistenceService = PersistenceService(app)
    val wgsUbamService = new WgsUbamService(persistenceService, searchService)

    val transferKey =
      TransferWgsUbamV1Key(Location.GCP, "barcode1", 2, "library3")
    val transferMetadata =
      TransferWgsUbamV1Metadata(
        project = Option("testProject"),
        notes = Option("notable update"),
        documentStatus = documentStatus
      )
    for {
      returnedUpsertId <- wgsUbamService.upsertMetadata(
        transferKey,
        transferMetadata
      )
    } yield {
      val expectedDocument = WgsUbamService.v1DocumentConverter
        .withMetadata(
          WgsUbamService.v1DocumentConverter.empty(transferKey),
          transferMetadata.copy(documentStatus = expectedDocumentStatus)
        )
        .copy(upsertId = returnedUpsertId)

      memoryPersistenceDAO.writeCalls should be(
        Seq((expectedDocument, ElasticsearchIndex.WgsUbam))
      )
      memorySearchDAO.updateCalls should be(
        Seq((expectedDocument, ElasticsearchIndex.WgsUbam))
      )
      memorySearchDAO.queryCalls should be(empty)
    }
  }
}
