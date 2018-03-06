package org.broadinstitute.clio.server.service

import java.net.URI

import akka.stream.scaladsl.Sink
import io.circe.syntax._
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ElasticsearchIndex,
  ElasticsearchUtil
}
import org.broadinstitute.clio.server.{MockClioApp, TestKitSuite}
import org.broadinstitute.clio.server.dataaccess.{MemoryPersistenceDAO, MemorySearchDAO}
import org.broadinstitute.clio.transfer.model.gvcf.{
  GvcfExtensions,
  TransferGvcfV1Key,
  TransferGvcfV1Metadata,
  TransferGvcfV1QueryInput
}
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.{DocumentStatus, Location, UpsertId}

class GvcfServiceSpec extends TestKitSuite("GvcfServiceSpec") with ModelAutoDerivation {
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

    val transferInput =
      TransferGvcfV1QueryInput(project = Option("testProject"))
    for {
      _ <- gvcfService.queryMetadata(transferInput).runWith(Sink.seq)
    } yield {
      memorySearchDAO.updateCalls should be(empty)
      memorySearchDAO.queryCalls should be(
        Seq(
          GvcfService.v1QueryConverter.buildQuery(
            transferInput.copy(documentStatus = Option(DocumentStatus.Normal))
          )(
            ElasticsearchIndex.Gvcf
          )
        )
      )
    }
  }

  private def upsertMetadataTest(
    documentStatus: Option[DocumentStatus],
    expectedDocumentStatus: Option[DocumentStatus]
  ) = {
    val index = ElasticsearchIndex.Gvcf

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
    val transferMetadata =
      TransferGvcfV1Metadata(
        gvcfPath = Option(
          URI.create(s"gs://path/gvcfPath${GvcfExtensions.GvcfExtension}")
        ),
        notes = Option("notable update"),
        documentStatus = documentStatus
      )
    for {
      returnedUpsertId <- gvcfService.upsertMetadata(
        transferKey,
        transferMetadata
      )
    } yield {
      val expectedDocument = GvcfService.v1DocumentConverter
        .document(
          transferKey,
          transferMetadata.copy(documentStatus = expectedDocumentStatus)
        )
        .deepMerge(
          Map(
            ElasticsearchUtil
              .toElasticsearchName(UpsertId.UpsertIdFieldName) -> returnedUpsertId
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
