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

class GvcfServiceSpec extends AsyncFlatSpec with Matchers {
  behavior of "GvcfService"

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
    val gvcfService = new GvcfService(persistenceService, searchService)

    val transferInputMapper =
      new CaseClassMapper[TransferGvcfV1QueryInput]
    val transferInput =
      transferInputMapper.newInstance(Map("project" -> Option("testProject")))
    for {
      _ <- gvcfService.queryMetadata(transferInput)
    } yield {
      memorySearchDAO.updateCalls should be(empty)
      memorySearchDAO.queryCalls should be(
        Seq(transferInput.copy(documentStatus = Some(DocumentStatus.Normal)))
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
    val gvcfService = new GvcfService(persistenceService, searchService)

    val transferKey =
      TransferGvcfV1Key(Location.GCP, "project1", "sample1", 1)
    val transferMetadataMapper =
      new CaseClassMapper[TransferGvcfV1Metadata]
    val transferMetadata =
      transferMetadataMapper.newInstance(
        Map(
          "gvcfPath" -> Option("gs://path/gvcfPath.gvcf"),
          "notes" -> Option("notable update"),
          "documentStatus" -> documentStatus
        )
      )
    for {
      returnedClioId <- gvcfService.upsertMetadata(
        transferKey,
        transferMetadata
      )
    } yield {
      val expectedDocument = GvcfService.v1DocumentConverter
        .withMetadata(
          GvcfService.v1DocumentConverter.empty(transferKey),
          transferMetadata.copy(documentStatus = expectedDocumentStatus)
        )
        .copy(clioId = returnedClioId)

      memoryPersistenceDAO.writeCalls should be(
        Seq((expectedDocument, ElasticsearchIndex.Gvcf))
      )
      memorySearchDAO.updateCalls should be(
        Seq(
          (
            GvcfService.v1DocumentConverter.id(transferKey),
            expectedDocument,
            ElasticsearchIndex.Gvcf
          )
        )
      )
      memorySearchDAO.queryCalls should be(empty)
    }
  }
}
