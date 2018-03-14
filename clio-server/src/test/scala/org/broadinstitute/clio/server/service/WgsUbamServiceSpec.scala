package org.broadinstitute.clio.server.service

import akka.stream.scaladsl.Sink
import io.circe.syntax._
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ElasticsearchIndex,
  ElasticsearchUtil
}
import org.broadinstitute.clio.server.{MockClioApp, TestKitSuite}
import org.broadinstitute.clio.server.dataaccess.{MemoryPersistenceDAO, MemorySearchDAO}
import org.broadinstitute.clio.transfer.model.ubam.{
  TransferUbamV1Key,
  TransferUbamV1Metadata,
  TransferUbamV1QueryInput
}
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

class WgsUbamServiceSpec
    extends TestKitSuite("WgsUbamServiceSpec")
    with ModelAutoDerivation {
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
      TransferUbamV1QueryInput(project = Option("testProject"))
    for {
      _ <- wgsUbamService.queryMetadata(transferInput).runWith(Sink.seq)
    } yield {
      memorySearchDAO.updateCalls should be(empty)
      memorySearchDAO.queryCalls should be(
        Seq(
          WgsUbamService.v1QueryConverter.buildQuery(
            transferInput.copy(documentStatus = Option(DocumentStatus.Normal))
          )(ElasticsearchIndex.WgsUbam)
        )
      )
    }
  }

  private def upsertMetadataTest(
    documentStatus: Option[DocumentStatus],
    expectedDocumentStatus: Option[DocumentStatus]
  ) = {
    val index = ElasticsearchIndex.WgsUbam

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
      TransferUbamV1Key(Location.GCP, "barcode1", 2, "library3")
    val transferMetadata =
      TransferUbamV1Metadata(
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
        .document(
          transferKey,
          transferMetadata.copy(documentStatus = expectedDocumentStatus)
        )
        .deepMerge(
          Map(
            ElasticsearchUtil
              .toElasticsearchName(ElasticsearchIndex.UpsertIdElasticsearchName) -> returnedUpsertId
          ).asJson
        )

      memoryPersistenceDAO.writeCalls should be(Seq((expectedDocument, index)))
      memorySearchDAO.updateCalls should be(
        Seq((Seq(expectedDocument), index))
      )
      memorySearchDAO.queryCalls should be(empty)
    }
  }
}
