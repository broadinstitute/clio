package org.broadinstitute.clio.server.service

import akka.NotUsed
import akka.stream.scaladsl.Source
import io.circe.Json
import org.broadinstitute.clio.server.dataaccess.elasticsearch._
import org.broadinstitute.clio.transfer.model.TransferIndex
import org.broadinstitute.clio.util.model.{DocumentStatus, UpsertId}

import scala.concurrent.{ExecutionContext, Future}

abstract class IndexService[TI <: TransferIndex](
  persistenceService: PersistenceService,
  searchService: SearchService,
  elasticsearchIndex: ElasticsearchIndex[TI],
  val transferIndex: TI
)(
  implicit
  executionContext: ExecutionContext
) {

  import transferIndex.implicits._

  private[service] val v1DocumentConverter =
    ElasticsearchDocumentMapper[
      transferIndex.KeyType,
      transferIndex.MetadataType
    ]

  private[service] val v1QueryConverter =
    ElasticsearchQueryMapper[
      transferIndex.QueryInputType
    ]

  def upsertMetadata(
    transferKey: transferIndex.KeyType,
    transferMetadata: transferIndex.MetadataType
  ): Future[UpsertId] = {
    val updatedTransferMetadata = transferMetadata.withDocumentStatus(
      transferMetadata.documentStatus.orElse(Some(DocumentStatus.Normal))
    )

    persistenceService
      .upsertMetadata(
        transferKey,
        updatedTransferMetadata,
        v1DocumentConverter,
        elasticsearchIndex
      )
  }

  def queryMetadata(
    transferInput: transferIndex.QueryInputType
  ): Source[Json, NotUsed] = {
    val transferInputNew =
      transferInput.withDocumentStatus(Option(DocumentStatus.Normal))
    queryAllMetadata(transferInputNew)
  }

  def queryAllMetadata(
    transferInput: transferIndex.QueryInputType
  ): Source[Json, NotUsed] = {
    searchService.queryMetadata(
      transferInput,
      v1QueryConverter
    )(
      elasticsearchIndex
    )
  }

  def querySchema(): Future[Json] = Future(transferIndex.jsonSchema)
}
