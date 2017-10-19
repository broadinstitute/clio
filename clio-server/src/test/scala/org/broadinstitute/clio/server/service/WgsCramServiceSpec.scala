package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.server.dataaccess.{
  MemoryPersistenceDAO,
  MemorySearchDAO
}
import org.broadinstitute.clio.server.{MockClioApp, TestKitSuite}
import org.broadinstitute.clio.transfer.model.wgscram.{
  TransferWgsCramV1Key,
  TransferWgsCramV1Metadata,
  TransferWgsCramV1QueryInput
}
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

class WgsCramServiceSpec extends TestKitSuite("WgsCramServiceSpec") {
  behavior of "WgsCramService"

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
    val cramService = new WgsCramService(persistenceService, searchService)

    val transferInput =
      TransferWgsCramV1QueryInput(project = Option("testProject"))
    for {
      _ <- cramService.queryMetadata(transferInput)
    } yield {
      memorySearchDAO.updateCalls should be(empty)
      memorySearchDAO.queryCalls should be(
        Seq(
          WgsCramService.v1QueryConverter.buildQuery(
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
    val cramService = new WgsCramService(persistenceService, searchService)

    val transferKey =
      TransferWgsCramV1Key(Location.GCP, "project1", "sample1", 1)
    val transferMetadata =
      TransferWgsCramV1Metadata(
        cramPath = Option("gs://path/cramPath.cram"),
        notes = Option("notable update"),
        documentStatus = documentStatus
      )
    for {
      returnedUpsertId <- cramService.upsertMetadata(
        transferKey,
        transferMetadata
      )
    } yield {
      val expectedDocument = WgsCramService.v1DocumentConverter
        .withMetadata(
          WgsCramService.v1DocumentConverter.empty(transferKey),
          transferMetadata.copy(documentStatus = expectedDocumentStatus)
        )
        .copy(upsertId = returnedUpsertId)

      memoryPersistenceDAO.writeCalls should be(
        Seq((expectedDocument, ElasticsearchIndex.WgsCram))
      )
      memorySearchDAO.updateCalls should be(
        Seq((expectedDocument, ElasticsearchIndex.WgsCram))
      )
      memorySearchDAO.queryCalls should be(empty)
    }
  }
}