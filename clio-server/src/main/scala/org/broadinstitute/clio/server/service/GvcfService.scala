package org.broadinstitute.clio.server.service

import akka.NotUsed
import akka.stream.scaladsl.Source
import io.circe.Json
import org.broadinstitute.clio.server.dataaccess.elasticsearch._
import org.broadinstitute.clio.transfer.model.GvcfIndex
import org.broadinstitute.clio.transfer.model.gvcf.{
  TransferGvcfV1Key,
  TransferGvcfV1Metadata,
  TransferGvcfV1QueryInput
}
import org.broadinstitute.clio.util.model.{DocumentStatus, UpsertId}

import scala.concurrent.{ExecutionContext, Future}

/**
  * Service responsible for performing all gvcf-specific logic
  * before handing off to the generic search / persistence services.
  */
class GvcfService(
  persistenceService: PersistenceService,
  searchService: SearchService
)(
  implicit executionContext: ExecutionContext,
  index: ElasticsearchIndex[GvcfIndex.type]
) {

  def upsertMetadata(
    transferKey: TransferGvcfV1Key,
    transferMetadata: TransferGvcfV1Metadata
  ): Future[UpsertId] = {
    val updatedTransferMetadata = transferMetadata.copy(
      documentStatus = transferMetadata.documentStatus.orElse(Some(DocumentStatus.Normal))
    )

    persistenceService
      .upsertMetadata(
        transferKey,
        updatedTransferMetadata,
        GvcfService.v1DocumentConverter,
        ElasticsearchIndex.Gvcf
      )
  }

  def queryMetadata(
    transferInput: TransferGvcfV1QueryInput
  ): Source[Json, NotUsed] = {
    val transferInputNew =
      transferInput.copy(documentStatus = Option(DocumentStatus.Normal))
    queryAllMetadata(transferInputNew)
  }

  def queryAllMetadata(
    transferInput: TransferGvcfV1QueryInput
  ): Source[Json, NotUsed] = {
    searchService.queryMetadata(
      transferInput,
      GvcfService.v1QueryConverter
    )
  }

  def querySchema(): Future[Json] = Future(GvcfIndex.jsonSchema)
}

object GvcfService {
  import GvcfIndex.implicits._

  private[service] val v1DocumentConverter =
    ElasticsearchDocumentMapper[
      GvcfIndex.KeyType,
      TransferGvcfV1Metadata,
    ]

  val v1QueryConverter: ElasticsearchQueryMapper[TransferGvcfV1QueryInput] =
    ElasticsearchQueryMapper[TransferGvcfV1QueryInput]
}
