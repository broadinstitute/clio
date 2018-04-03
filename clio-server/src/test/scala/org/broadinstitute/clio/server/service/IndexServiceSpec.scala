package org.broadinstitute.clio.server.service

import akka.stream.scaladsl.Sink
import com.sksamuel.elastic4s.searches.queries.SimpleStringQueryDefinition
import io.circe.syntax._
import org.broadinstitute.clio.server.TestKitSuite
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.server.dataaccess.{MemoryPersistenceDAO, MemorySearchDAO}
import org.broadinstitute.clio.transfer.model.ClioIndex
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.DocumentStatus

abstract class IndexServiceSpec[
  CI <: ClioIndex
](specificService: String)
    extends TestKitSuite(specificService + "Spec")
    with ModelAutoDerivation {

  val memoryPersistenceDAO = new MemoryPersistenceDAO()
  val memorySearchDAO = new MemorySearchDAO()

  val indexService: IndexService[CI] = {

    val searchService = new SearchService(memorySearchDAO)
    val persistenceService = new PersistenceService(memoryPersistenceDAO, memorySearchDAO)
    getService(persistenceService, searchService)
  }
  import indexService.clioIndex.implicits._

  def elasticsearchIndex: ElasticsearchIndex[CI]
  def dummyKey: indexService.clioIndex.KeyType
  def dummyInput: indexService.clioIndex.QueryInputType

  def getDummyMetadata(
    documentStatus: Option[DocumentStatus]
  ): indexService.clioIndex.MetadataType

  def getService(
    persistenceService: PersistenceService,
    searchService: SearchService
  ): IndexService[CI]

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
          indexService.queryConverter.buildQuery(
            dummyInput.withDocumentStatus(Option(DocumentStatus.Normal))
          )(elasticsearchIndex)
        )
      )
    }
  }

  private def upsertMetadataTest(
    documentStatus: Option[DocumentStatus]
  ) = {
    val metadata = getDummyMetadata(documentStatus)
    val expectedDocumentStatus = Option(documentStatus.getOrElse(DocumentStatus.Normal))
    for {
      returnedUpsertId <- indexService.upsertMetadata(
        dummyKey,
        metadata
      )
    } yield {
      val expectedDocument =
        indexService.documentConverter
          .document(
            dummyKey,
            metadata.withDocumentStatus(expectedDocumentStatus)
          )
          .mapObject(
            _.add(ElasticsearchIndex.UpsertIdElasticsearchName, returnedUpsertId.asJson)
          )

      memoryPersistenceDAO.writeCalls should be(
        Seq((expectedDocument, elasticsearchIndex))
      )
      memorySearchDAO.updateCalls should be(
        Seq((Seq(expectedDocument), elasticsearchIndex))
      )
      memorySearchDAO.queryCalls should be(
        Seq(SimpleStringQueryDefinition(dummyKey.asJson.toString))
      )
    }
  }

  private def clearMemory(): Unit = {
    memoryPersistenceDAO.writeCalls.clear()
    memorySearchDAO.updateCalls.clear()
    memorySearchDAO.queryCalls.clear()
  }
}
