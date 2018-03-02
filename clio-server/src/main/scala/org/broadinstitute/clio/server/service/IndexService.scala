package org.broadinstitute.clio.server.service

import akka.NotUsed
import akka.stream.scaladsl.Source
import io.circe.Json
import org.broadinstitute.clio.server.dataaccess.elasticsearch._
import org.broadinstitute.clio.transfer.model.TransferIndex
import org.broadinstitute.clio.util.model.{DocumentStatus, UpsertId}

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

abstract class IndexService[TI <: TransferIndex, D <: ClioDocument: ElasticsearchIndex](
  persistenceService: PersistenceService,
  searchService: SearchService,
  val transferIndex: TI,
  elasticsearchIndex: ElasticsearchIndex[D]
)(
  implicit
  executionContext: ExecutionContext,
  documentTag: ClassTag[D]
) {

  import transferIndex.implicits._

  private[service] val v1DocumentConverter =
    AutoElasticsearchDocumentMapper[
      transferIndex.KeyType,
      transferIndex.MetadataType,
      D
    ]

  val v1QueryConverter: ElasticsearchQueryMapper[
    transferIndex.QueryInputType,
    transferIndex.QueryOutputType,
    D
  ] =
    AutoElasticsearchQueryMapper[
      transferIndex.QueryInputType,
      transferIndex.QueryOutputType,
      D
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
  ): Source[transferIndex.QueryOutputType, NotUsed] = {
    val transferInputNew =
      transferInput.withDocumentStatus(Option(DocumentStatus.Normal))
    queryAllMetadata(transferInputNew)
  }

  def queryAllMetadata(
    transferInput: transferIndex.QueryInputType
  ): Source[transferIndex.QueryOutputType, NotUsed] = {
    searchService.queryMetadata(
      transferInput,
      v1QueryConverter,
      elasticsearchIndex
    )
  }

  def querySchema(): Future[Json] = Future(transferIndex.jsonSchema)
}
