package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.MockClioApp
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.server.dataaccess.{
  MemoryPersistenceDAO,
  MemorySearchDAO
}
import org.broadinstitute.clio.transfer.model._
import org.broadinstitute.clio.util.generic.CaseClassMapper
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

import org.scalatest.{AsyncFlatSpec, Matchers}

class WgsUbamServiceSpec extends AsyncFlatSpec with Matchers {
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

    val transferInputMapper =
      new CaseClassMapper[TransferWgsUbamV1QueryInput]
    val transferInput =
      transferInputMapper.newInstance(Map("project" -> Option("testProject")))
    for {
      _ <- wgsUbamService.queryMetadata(transferInput)
    } yield {
      memorySearchDAO.updateCalls should be(empty)
      memorySearchDAO.queryCalls should be(
        Seq(
          WgsUbamService.v1QueryConverter.buildQuery(
            transferInput.copy(documentStatus = Some(DocumentStatus.Normal))
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
      TransferWgsUbamV1Key("barcode1", 2, "library3", Location.GCP)
    val transferMetadataMapper =
      new CaseClassMapper[TransferWgsUbamV1Metadata]
    val transferMetadata =
      transferMetadataMapper.newInstance(
        Map(
          "project" -> Option("testProject"),
          "notes" -> Option("notable update"),
          "documentStatus" -> documentStatus
        )
      )
    for {
      returnedClioId <- wgsUbamService.upsertMetadata(
        transferKey,
        transferMetadata
      )
    } yield {
      val expectedDocument = WgsUbamService.v1DocumentConverter
        .withMetadata(
          WgsUbamService.v1DocumentConverter.empty(transferKey),
          transferMetadata.copy(documentStatus = expectedDocumentStatus)
        )
        .copy(clioId = returnedClioId)

      memoryPersistenceDAO.writeCalls should be(
        Seq((expectedDocument, ElasticsearchIndex.WgsUbam))
      )
      memorySearchDAO.updateCalls should be(
        Seq(
          (
            WgsUbamService.v1DocumentConverter.id(transferKey),
            expectedDocument,
            ElasticsearchIndex.WgsUbam
          )
        )
      )
      memorySearchDAO.queryCalls should be(empty)
    }
  }
}
