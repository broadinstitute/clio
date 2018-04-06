package org.broadinstitute.clio.server.service

import java.security.SecureRandom

import akka.stream.scaladsl.Sink
import com.sksamuel.elastic4s.searches.queries.BoolQueryDefinition
import io.circe.syntax._
import org.broadinstitute.clio.server.TestKitSuite
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.server.dataaccess.{MemoryPersistenceDAO, MemorySearchDAO}
import org.broadinstitute.clio.server.exceptions.UpsertValidationException
import org.broadinstitute.clio.transfer.model.ClioIndex
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.DocumentStatus
import org.broadinstitute.clio.util.model.DocumentStatus.{Deleted, Normal}
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

  def randomString: String =
    util.Random.javaRandomToRandom(new SecureRandom()).nextString(10)

  def elasticsearchIndex: ElasticsearchIndex[CI]
  def dummyKey: indexService.clioIndex.KeyType
  def dummyKeyQuery: BoolQueryDefinition
  def dummyInput: indexService.clioIndex.QueryInputType

  def getDummyMetadata(
    documentStatus: Option[DocumentStatus]
  ): indexService.clioIndex.MetadataType

  def copyDummyMetadataChangeField(
    metadata: indexService.clioIndex.MetadataType
  ): indexService.clioIndex.MetadataType

  def getService(
    persistenceService: PersistenceService,
    searchService: SearchService
  ): IndexService[CI]

  behavior of specificService

  it should "fail if the update would overwrite data" in {
    recoverToSucceededIf[UpsertValidationException] {
      clearMemory()
      overMetadataWriteTest(None, None)
    }
  }

  it should "succeed if the update would overwrite data and force is set to true" in {
    clearMemory()
    overMetadataWriteTest(Some(Normal), Some(Normal), force = true)
  }

  it should "not overwrite document status if it is not set" in {
    clearMemory()
    overMetadataWriteTest(Some(Deleted), None, changeField = false)
  }

  it should "not overwrite document status if it is not set and force is set to true" in {
    clearMemory()
    overMetadataWriteTest(Some(Deleted), None, changeField = false, force = true)
  }

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
      returnedUpsertId <- indexService
        .upsertMetadata(
          dummyKey,
          metadata
        )
        .runWith(Sink.head)
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
        Seq(dummyKeyQuery)
      )
    }
  }

  private def overMetadataWriteTest(
    originalStatus: Option[DocumentStatus],
    newStatus: Option[DocumentStatus],
    changeField: Boolean = true,
    force: Boolean = false
  ) = {

    val metadata = getDummyMetadata(originalStatus)
    val newMetadata = if (changeField) {
      copyDummyMetadataChangeField(metadata).withDocumentStatus(newStatus)
    } else {
      metadata.withDocumentStatus(newStatus)
    }

    val expectedOriginalDocumentStatus = Option(originalStatus.getOrElse(Normal))
    val expectedNewDocumentStatus = Option(
      newStatus.getOrElse(originalStatus.getOrElse(Normal))
    )
    for {
      upsertId <- indexService
        .upsertMetadata(
          dummyKey,
          metadata
        )
        .runWith(Sink.head)
      newUpsertId <- indexService
        .upsertMetadata(dummyKey, newMetadata, force)
        .runWith(Sink.head)
    } yield {

      val expectedDocument =
        indexService.documentConverter
          .document(
            dummyKey,
            metadata.withDocumentStatus(expectedOriginalDocumentStatus)
          )
          .mapObject(
            _.add(ElasticsearchIndex.UpsertIdElasticsearchName, upsertId.asJson)
          )

      val expectedDocument2 =
        indexService.documentConverter
          .document(
            dummyKey,
            newMetadata.withDocumentStatus(expectedNewDocumentStatus)
          )
          .mapObject(
            _.add(ElasticsearchIndex.UpsertIdElasticsearchName, newUpsertId.asJson)
          )

      memoryPersistenceDAO.writeCalls should be(
        Seq(
          (expectedDocument, elasticsearchIndex),
          (expectedDocument2, elasticsearchIndex)
        )
      )
      memorySearchDAO.updateCalls should be(
        Seq(
          (Seq(expectedDocument), elasticsearchIndex),
          (Seq(expectedDocument2), elasticsearchIndex)
        )
      )
      memorySearchDAO.queryCalls should be(
        if (newStatus.isEmpty) Seq(dummyKeyQuery, dummyKeyQuery) else Seq(dummyKeyQuery)
      )
    }
  }

  private def clearMemory(): Unit = {
    memoryPersistenceDAO.writeCalls.clear()
    memorySearchDAO.updateCalls.clear()
    memorySearchDAO.queryCalls.clear()
  }
}
