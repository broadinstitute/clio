package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.dataaccess.elasticsearch._
import Elastic4sAutoDerivation._
import org.broadinstitute.clio.transfer.model._
import org.broadinstitute.clio.util.json.JsonSchemas
import org.broadinstitute.clio.util.model.DocumentStatus

import com.sksamuel.elastic4s.circe._
import io.circe.Json

import scala.concurrent.{ExecutionContext, Future}

import java.util.UUID

class GvcfService(
  persistenceService: PersistenceService,
  searchService: SearchService
)(implicit executionContext: ExecutionContext) {

  def upsertMetadata(transferKey: TransferGvcfV1Key,
                     transferMetadata: TransferGvcfV1Metadata): Future[UUID] = {
    val updatedTransferMetadata = transferMetadata.copy(
      documentStatus =
        transferMetadata.documentStatus.orElse(Some(DocumentStatus.Normal))
    )

    persistenceService
      .upsertMetadata(
        transferKey,
        updatedTransferMetadata,
        ElasticsearchIndex.Gvcf,
        GvcfService.v1DocumentConverter
      )
  }

  def queryMetadata(
    transferInput: TransferGvcfV1QueryInput
  ): Future[Seq[TransferGvcfV1QueryOutput]] = {
    val transferInputNew =
      transferInput.copy(documentStatus = Option(DocumentStatus.Normal))
    queryAllMetadata(transferInputNew)
  }

  def queryAllMetadata(
    transferInput: TransferGvcfV1QueryInput
  ): Future[Seq[TransferGvcfV1QueryOutput]] = {
    searchService.queryMetadata(
      transferInput,
      ElasticsearchIndex.Gvcf,
      GvcfService.v1QueryConverter
    )
  }

  def querySchema(): Future[Json] = Future(JsonSchemas.Gvcf)
}

object GvcfService {
  private[service] val v1DocumentConverter =
    AutoElasticsearchDocumentMapper[
      TransferGvcfV1Key,
      TransferGvcfV1Metadata,
      DocumentGvcf
    ]

  private[service] val v1QueryConverter =
    AutoElasticsearchQueryMapper[
      TransferGvcfV1QueryInput,
      TransferGvcfV1QueryOutput,
      DocumentGvcf
    ]
}