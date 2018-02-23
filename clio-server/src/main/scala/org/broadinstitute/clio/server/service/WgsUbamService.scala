package org.broadinstitute.clio.server.service

import akka.NotUsed
import akka.stream.scaladsl.Source
import io.circe.Json
import org.broadinstitute.clio.server.dataaccess.elasticsearch._
import org.broadinstitute.clio.transfer.model.WgsUbamIndex
import org.broadinstitute.clio.transfer.model.ubam.{
  TransferUbamV1Key,
  TransferUbamV1Metadata,
  TransferUbamV1QueryInput,
  TransferUbamV1QueryOutput
}
import org.broadinstitute.clio.util.model.{DocumentStatus, UpsertId}

import scala.concurrent.{ExecutionContext, Future}

/**
  * Service responsible for performing all wgs-ubam-specific logic
  * before handing off to the generic search / persistence services.
  */
class WgsUbamService(
  persistenceService: PersistenceService,
  searchService: SearchService
)(implicit executionContext: ExecutionContext) {

  def upsertMetadata(
    transferKey: TransferUbamV1Key,
    transferMetadata: TransferUbamV1Metadata
  ): Future[UpsertId] = {
    val updatedTransferMetadata = transferMetadata.copy(
      documentStatus = transferMetadata.documentStatus.orElse(Some(DocumentStatus.Normal))
    )

    persistenceService
      .upsertMetadata(
        transferKey,
        transferMetadata,
        updatedTransferMetadata,
        WgsUbamService.v1DocumentConverter
      )
  }

  def queryMetadata(
    transferInput: TransferUbamV1QueryInput
  ): Source[TransferUbamV1QueryOutput, NotUsed] = {
    val transferInputNew =
      transferInput.copy(documentStatus = Option(DocumentStatus.Normal))
    queryAllMetadata(transferInputNew)
  }

  def queryAllMetadata(
    transferInput: TransferUbamV1QueryInput
  ): Source[TransferUbamV1QueryOutput, NotUsed] = {
    searchService.queryMetadata(
      transferInput,
      WgsUbamService.v1QueryConverter
    )
  }

  def querySchema(): Future[Json] = Future(WgsUbamIndex.jsonSchema)
}

object WgsUbamService {
  private[service] val v1DocumentConverter =
    AutoElasticsearchDocumentMapper[
      TransferUbamV1Key,
      TransferUbamV1Metadata,
    ]

  val v1QueryConverter: ElasticsearchQueryMapper[
    TransferUbamV1QueryInput,
    TransferUbamV1QueryOutput,
    TransferUbamV1Key,
    TransferUbamV1Metadata,
  ] =
    AutoElasticsearchQueryMapper[
      TransferUbamV1QueryInput,
      TransferUbamV1QueryOutput,
      TransferUbamV1Key,
      TransferUbamV1Metadata,
    ]
}
