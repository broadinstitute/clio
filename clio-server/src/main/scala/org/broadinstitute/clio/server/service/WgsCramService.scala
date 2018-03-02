package org.broadinstitute.clio.server.service

import akka.NotUsed
import akka.stream.scaladsl.Source
import io.circe.Json
import org.broadinstitute.clio.server.dataaccess.elasticsearch._
import org.broadinstitute.clio.transfer.model.WgsCramIndex
import org.broadinstitute.clio.transfer.model.wgscram.{
  TransferWgsCramV1Key,
  TransferWgsCramV1Metadata,
  TransferWgsCramV1QueryInput
}
import org.broadinstitute.clio.util.model.{DocumentStatus, UpsertId}

import scala.concurrent.{ExecutionContext, Future}

/**
  * Service responsible for performing all wgs-cram-specific logic
  * before handing off to the generic search / persistence services.
  */
class WgsCramService(
  persistenceService: PersistenceService,
  searchService: SearchService
)(implicit executionContext: ExecutionContext, index: ElasticsearchIndex[WgsCramIndex.type]) {

  def upsertMetadata(
    transferKey: TransferWgsCramV1Key,
    transferMetadata: TransferWgsCramV1Metadata
  ): Future[UpsertId] = {
    val updatedTransferMetadata = transferMetadata.copy(
      documentStatus = transferMetadata.documentStatus.orElse(Some(DocumentStatus.Normal))
    )

    persistenceService
      .upsertMetadata(
        transferKey,
        updatedTransferMetadata,
        WgsCramService.v1DocumentConverter
      )
  }

  def queryMetadata(
    transferInput: TransferWgsCramV1QueryInput
  ): Source[Json, NotUsed] = {
    val transferInputNew =
      transferInput.copy(documentStatus = Option(DocumentStatus.Normal))
    queryAllMetadata(transferInputNew)
  }

  def queryAllMetadata(
    transferInput: TransferWgsCramV1QueryInput
  ): Source[Json, NotUsed] = {
    searchService.queryMetadata(
      transferInput,
      WgsCramService.v1QueryConverter
    )
  }

  def querySchema(): Future[Json] = Future(WgsCramIndex.jsonSchema)
}

object WgsCramService {
  private[service] val v1DocumentConverter =
    ElasticsearchDocumentMapper[
      TransferWgsCramV1Key,
      TransferWgsCramV1Metadata,
    ]

  val v1QueryConverter: ElasticsearchQueryMapper[
    TransferWgsCramV1QueryInput,
    WgsCramIndex.type,
  ] =
    ElasticsearchQueryMapper[
      TransferWgsCramV1QueryInput,
      WgsCramIndex.type,
    ]
}
