package org.broadinstitute.clio.server.service

import java.security.SecureRandom

import akka.stream.scaladsl.Sink
import akka.stream.testkit.scaladsl.TestSink
import com.sksamuel.elastic4s.searches.queries.SimpleStringQueryDefinition
import io.circe.syntax._
import org.broadinstitute.clio.server.TestKitSuite
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.server.dataaccess.{MemoryPersistenceDAO, MemorySearchDAO}
import org.broadinstitute.clio.server.exceptions.UpsertValidationException
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

  def randomString: String =
    util.Random.javaRandomToRandom(new SecureRandom()).nextString(10)

  def elasticsearchIndex: ElasticsearchIndex[CI]
  def dummyKey: indexService.clioIndex.KeyType
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
      val metadata = getDummyMetadata(None)
      for {
        _ <- indexService
          .upsertMetadata(
            dummyKey,
            metadata
          )
          .runWith(Sink.head)
      } yield {
        throw indexService
          .upsertMetadata(dummyKey, copyDummyMetadataChangeField(metadata))
          .runWith(TestSink.probe)
          .expectSubscriptionAndError()
      }
    }
  }

  it should "succeed if the update would overwrite data and force is set to true" in {

    clearMemory()
    val metadata = getDummyMetadata(Option(DocumentStatus.Normal))
    val newMetadata = copyDummyMetadataChangeField(metadata)

    val expectedDocumentStatus = Option(DocumentStatus.Normal)
    for {
      upsertId <- indexService
        .upsertMetadata(
          dummyKey,
          metadata
        )
        .runWith(Sink.head)
      newUpsertId <- indexService
        .upsertMetadata(dummyKey, newMetadata, force = true)
        .runWith(Sink.head)
    } yield {

      val expectedDocument =
        indexService.documentConverter
          .document(
            dummyKey,
            metadata.withDocumentStatus(expectedDocumentStatus)
          )
          .mapObject(
            _.add(ElasticsearchIndex.UpsertIdElasticsearchName, upsertId.asJson)
          )

      val expectedDocument2 =
        indexService.documentConverter
          .document(
            dummyKey,
            newMetadata.withDocumentStatus(expectedDocumentStatus)
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
        Seq(
          SimpleStringQueryDefinition(dummyKey.asJson.noSpaces),
          SimpleStringQueryDefinition(dummyKey.asJson.noSpaces)
        )
      )
    }

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
        Seq(SimpleStringQueryDefinition(dummyKey.asJson.noSpaces))
      )
    }
  }

  private def clearMemory(): Unit = {
    memoryPersistenceDAO.writeCalls.clear()
    memorySearchDAO.updateCalls.clear()
    memorySearchDAO.queryCalls.clear()
  }
}
