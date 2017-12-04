package org.broadinstitute.clio.server.service

import akka.NotUsed
import akka.stream.scaladsl.Source
import io.circe.Json
import org.broadinstitute.clio.server.dataaccess.elasticsearch._
import org.broadinstitute.clio.transfer.model.WgsUbamIndex
import org.broadinstitute.clio.transfer.model.wgsubam.{
  TransferWgsUbamV1Key,
  TransferWgsUbamV1Metadata,
  TransferWgsUbamV1QueryInput,
  TransferWgsUbamV1QueryOutput
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
    transferKey: TransferWgsUbamV1Key,
    transferMetadata: TransferWgsUbamV1Metadata
  ): Future[UpsertId] = {
    val updatedTransferMetadata = transferMetadata.copy(
      documentStatus = transferMetadata.documentStatus.orElse(Some(DocumentStatus.Normal))
    )

    persistenceService
      .upsertMetadata(
        transferKey,
        updatedTransferMetadata,
        ElasticsearchIndex.WgsUbam,
        WgsUbamService.v1DocumentConverter
      )
  }

  def queryMetadata(
    transferInput: TransferWgsUbamV1QueryInput
  ): Source[TransferWgsUbamV1QueryOutput, NotUsed] = {
    val transferInputNew =
      transferInput.copy(documentStatus = Option(DocumentStatus.Normal))
    queryAllMetadata(transferInputNew)
  }

  def queryAllMetadata(
    transferInput: TransferWgsUbamV1QueryInput
  ): Source[TransferWgsUbamV1QueryOutput, NotUsed] = {
    searchService.queryMetadata(
      transferInput,
      ElasticsearchIndex.WgsUbam,
      WgsUbamService.v1QueryConverter
    )
  }

  def querySchema(): Future[Json] = Future(WgsUbamIndex.jsonSchema)
}

object WgsUbamService {
  private[service] val v1DocumentConverter =
    AutoElasticsearchDocumentMapper[
      TransferWgsUbamV1Key,
      TransferWgsUbamV1Metadata,
      DocumentWgsUbam
    ]

  val v1QueryConverter: ElasticsearchQueryMapper[
    TransferWgsUbamV1QueryInput,
    TransferWgsUbamV1QueryOutput,
    DocumentWgsUbam
  ] =
    AutoElasticsearchQueryMapper[
      TransferWgsUbamV1QueryInput,
      TransferWgsUbamV1QueryOutput,
      DocumentWgsUbam
    ]
}
