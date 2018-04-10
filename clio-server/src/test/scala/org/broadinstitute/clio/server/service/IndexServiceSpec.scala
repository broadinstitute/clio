package org.broadinstitute.clio.server.service

import java.security.SecureRandom
import java.time.OffsetDateTime

import akka.NotUsed
import akka.stream.scaladsl.{Sink, Source}
import com.sksamuel.elastic4s.searches.queries.BoolQueryDefinition
import io.circe.Json
import io.circe.syntax._
import org.broadinstitute.clio.server.TestKitSuite
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.server.dataaccess.{PersistenceDAO, SearchDAO}
import org.broadinstitute.clio.server.exceptions.UpsertValidationException
import org.broadinstitute.clio.transfer.model.ClioIndex
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.DocumentStatus

import scala.concurrent.{ExecutionContext, Future}

abstract class IndexServiceSpec[
  CI <: ClioIndex
](specificService: String)
    extends TestKitSuite(specificService + "Spec")
    with ModelAutoDerivation {

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
      val metadata = getDummyMetadata(None)
      val alteredMetadata = copyDummyMetadataChangeField(metadata)
      expectRawQuery(Source.single(alteredMetadata.asJson))
      indexService
        .upsertMetadata(dummyKey, metadata)
        .runWith(Sink.ignore)
    }
  }

  it should "succeed if the update would overwrite data and force is set to true" in {
    val metadata = getDummyMetadata(Option(DocumentStatus.Normal))
    val alteredMetadata = copyDummyMetadataChangeField(metadata)
    val expectedDocument =
      indexService.documentConverter
        .document(
          dummyKey,
          metadata.withDocumentStatus(Option(DocumentStatus.Normal))
        )
    expectWriteUpdate(expectedDocument)
    expectUpdateMetadata(expectedDocument)
    expectRawQuery(Source.single(alteredMetadata.asJson))
    for {
      _ <- indexService
        .upsertMetadata(dummyKey, metadata, force = true)
        .runWith(Sink.head)
    } yield succeed

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

  it should "queryMetadata" in {
    expectRawQuery(Source.empty[Json])
    for {
      _ <- indexService.queryMetadata(dummyInput).runWith(Sink.seq)
    } yield succeed
  }

  it should "not update search if writing to storage fails" in {
    val metadata = getDummyMetadata(Some(DocumentStatus.Normal))
    val expectedDocument = indexService.documentConverter.document(dummyKey, metadata)
    expectRawQuery(Source.empty[Json])
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

  private def keyToJsonString(key: indexService.clioIndex.KeyType): String = {
    indexService.keyQueryConverter.buildQuery(key)(elasticsearchIndex)
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
    returnValue: Future[Unit] = Future.successful(())
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
      .returning(returnValue)
  }

  private def expectUpdateMetadata(
    expectedDocument: Json,
    returnValue: Future[Unit] = Future.successful(())
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
      .returning(returnValue)
  }
  private def expectRawQuery(
    returnValue: Source[Json, NotUsed]
  ) = {
    (mockSearchDAO
      .rawQuery(_: String)(_: ElasticsearchIndex[_]))
      .expects(keyToJsonString(dummyKey), elasticsearchIndex)
      .returning(returnValue)
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
    expectRawQuery(Source.empty[Json])
    for {
      _ <- indexService
        .upsertMetadata(
          dummyKey,
          metadata
        )
        .runWith(Sink.head)
    } yield succeed
  }
}
