package org.broadinstitute.clio.server.service

import java.security.SecureRandom
import java.time.OffsetDateTime

import akka.NotUsed
import akka.stream.scaladsl.{Sink, Source}
import com.sksamuel.elastic4s.searches.queries.BoolQueryDefinition
import io.circe.{Json, JsonObject}
import io.circe.syntax._
import org.broadinstitute.clio.server.TestKitSuite
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.server.dataaccess.{PersistenceDAO, SearchDAO}
import org.broadinstitute.clio.server.exceptions.UpsertValidationException
import org.broadinstitute.clio.transfer.model.ClioIndex
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.DocumentStatus
import org.broadinstitute.clio.util.model.DocumentStatus.{Deleted, Normal}
import org.scalatest.OneInstancePerTest

import scala.concurrent.{ExecutionContext, Future}

abstract class IndexServiceSpec[
  CI <: ClioIndex
](specificService: String)
    extends TestKitSuite(specificService + "Spec")
    with ModelAutoDerivation
    with OneInstancePerTest {

  val mockPersistenceDAO: PersistenceDAO = mock[PersistenceDAO]
  val mockSearchDAO: SearchDAO = mock[SearchDAO]

  val indexService: IndexService[CI] = getService(mockPersistenceDAO, mockSearchDAO)

  import indexService.clioIndex.implicits._

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
    persistenceDAO: PersistenceDAO,
    searchDAO: SearchDAO
  ): IndexService[CI]

  behavior of specificService

  it should "fail if the update would overwrite data" in {
    recoverToSucceededIf[UpsertValidationException] {
      overMetadataWriteTest(None, None, expectQueryOnly = true)
    }
  }

  it should "succeed if the update would overwrite data and force is set to true" in {
    overMetadataWriteTest(Some(Normal), Some(Normal), force = true)
  }

  it should "not overwrite document status if it is not set" in {
    overMetadataWriteTest(Some(Deleted), None, changeField = false)
  }

  it should "fail if it would overwrite document status" in {
    recoverToSucceededIf[UpsertValidationException] {
      overMetadataWriteTest(
        Some(Deleted),
        Some(Normal),
        changeField = false,
        expectQueryOnly = true
      )
    }
  }

  it should "not overwrite document status if it is not set and force is set to true" in {
    overMetadataWriteTest(Some(Deleted), None, changeField = false, force = true)
  }

  it should "upsertMetadata" in {
    upsertMetadataTest(None)
  }

  it should "upsertMetadata with document_status explicitly set to Normal" in {
    upsertMetadataTest(
      Option(DocumentStatus.Normal)
    )
  }

  it should "upsertMetadata with document_status explicitly set to Deleted" in {
    upsertMetadataTest(
      Option(DocumentStatus.Deleted)
    )
  }

  it should "forward a raw JSON string to the searchDAO" in {
    val json = JsonObject("this" -> "is valid json".asJson)
    expectRawQuery(
      json,
      Source.empty[Json]
    )
    for {
      _ <- indexService.rawQuery(json).runWith(Sink.ignore)
    } yield succeed
  }

  it should "convert and execute a queryMetadata call as a raw query" in {
    expectRawQuery(
      inputToJsonObject(dummyInput),
      Source.empty[Json]
    )
    for {
      _ <- indexService.queryMetadata(dummyInput).runWith(Sink.ignore)
    } yield succeed
  }

  it should "not update search if writing to storage fails" in {
    val metadata = getDummyMetadata(Some(DocumentStatus.Normal))
    val expectedDocument = indexService.documentConverter.document(dummyKey, metadata)
    expectRawQuery(
      keyToJsonObject(dummyKey),
      Source.empty[Json]
    )
    expectWriteUpdate(
      expectedDocument,
      Future.failed(new Exception("Write Failure Test"))
    )
    recoverToSucceededIf[Exception] {
      indexService
        .upsertMetadata(dummyKey, metadata)
        .runWith(Sink.ignore)
    }
  }

  private def keyToJsonObject(key: indexService.clioIndex.KeyType): JsonObject = {
    indexService.keyQueryConverter.buildQuery(key)(elasticsearchIndex)
  }

  private def inputToJsonObject(
    input: indexService.clioIndex.QueryInputType
  ): JsonObject = {
    indexService.queryConverter.buildQuery(input)(elasticsearchIndex)
  }

  private def jsonWithoutUpsertId(json: Json): Json = {
    json.mapObject { obj =>
      obj.remove(ElasticsearchIndex.UpsertIdElasticsearchName)
    }
  }

  private def jsonSimilarAndIndexEqual(
    expectedJson: Json,
    expectedIndex: ElasticsearchIndex[_]
  )(
    json: Json,
    index: ElasticsearchIndex[_]
  ): Boolean = {
    jsonWithoutUpsertId(expectedJson)
      .equals(
        jsonWithoutUpsertId(json)
      ) && expectedIndex.equals(index)
  }

  private def expectWriteUpdate(
    expectedDocument: Json,
    returning: Future[Unit] = Future.successful(())
  ) = {
    // we don't know what upsertId to expect, so need to use
    // a custom matcher that is upsertId-agnostic
    (mockPersistenceDAO
      .writeUpdate(
        _: Json,
        _: ElasticsearchIndex[_],
        _: OffsetDateTime
      )(
        _: ExecutionContext
      ))
      .expects(
        where { (document, index, _, _) =>
          jsonSimilarAndIndexEqual(
            expectedDocument,
            elasticsearchIndex
          )(document, index)
        }
      )
      .returning(returning)
  }

  private def expectUpdateMetadata(
    expectedDocument: Json,
    returning: Future[Unit] = Future.successful(())
  ) = {
    (mockSearchDAO
      .updateMetadata(_: Json)(_: ElasticsearchIndex[_]))
      .expects(
        where(
          jsonSimilarAndIndexEqual(
            expectedDocument,
            elasticsearchIndex
          )(_, _)
        )
      )
      .returning(returning)
  }

  private def expectRawQuery(
    expectedJson: JsonObject,
    returning: Source[Json, NotUsed]
  ) = {
    (mockSearchDAO
      .rawQuery(_: JsonObject)(_: ElasticsearchIndex[_]))
      .expects(expectedJson, elasticsearchIndex)
      .returning(returning)
  }

  private def upsertMetadataTest(
    documentStatus: Option[DocumentStatus]
  ) = {
    val metadata = getDummyMetadata(documentStatus)
    val expectedDocumentStatus = Option(documentStatus.getOrElse(DocumentStatus.Normal))
    val expectedDocument =
      indexService.documentConverter
        .document(
          dummyKey,
          metadata.withDocumentStatus(expectedDocumentStatus)
        )
    expectWriteUpdate(expectedDocument)
    expectUpdateMetadata(expectedDocument)
    expectRawQuery(
      keyToJsonObject(dummyKey),
      Source.empty[Json]
    )
    for {
      _ <- indexService
        .upsertMetadata(
          dummyKey,
          metadata
        )
        .runWith(Sink.head)
    } yield succeed
  }

  private def overMetadataWriteTest(
    originalStatus: Option[DocumentStatus],
    newStatus: Option[DocumentStatus],
    changeField: Boolean = true,
    force: Boolean = false,
    expectQueryOnly: Boolean = false
  ) = {

    val dummyMetadata = getDummyMetadata(originalStatus)
    val newMetadata = if (changeField) {
      copyDummyMetadataChangeField(dummyMetadata).withDocumentStatus(newStatus)
    } else {
      dummyMetadata.withDocumentStatus(newStatus)
    }
    val originalDocumentStatus = Option(originalStatus.getOrElse(Normal))
    val expectedNewDocumentStatus = Option(
      newStatus.getOrElse(originalStatus.getOrElse(Normal))
    )
    val originalMetadata = dummyMetadata.withDocumentStatus(originalDocumentStatus)
    val expectedDocument =
      indexService.documentConverter
        .document(
          dummyKey,
          newMetadata.withDocumentStatus(expectedNewDocumentStatus)
        )

    if (!force || newStatus.isEmpty) {
      expectRawQuery(
        keyToJsonObject(dummyKey),
        returning = Source.single(originalMetadata.asJson)
      )
    }
    if (!expectQueryOnly) {
      expectUpdateMetadata(expectedDocument)
      expectWriteUpdate(expectedDocument)
    }

    for {
      _ <- indexService
        .upsertMetadata(dummyKey, newMetadata, force)
        .runWith(Sink.head)
    } yield succeed
  }
}
