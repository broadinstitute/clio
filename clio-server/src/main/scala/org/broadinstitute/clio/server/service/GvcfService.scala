package org.broadinstitute.clio.server.service

import com.sksamuel.elastic4s.circe._
import io.circe.Json
import org.broadinstitute.clio.server.dataaccess.elasticsearch._
import org.broadinstitute.clio.transfer.model.GvcfIndex
import org.broadinstitute.clio.transfer.model.gvcf.{
  TransferGvcfV1Key,
  TransferGvcfV1Metadata,
  TransferGvcfV1QueryInput,
  TransferGvcfV1QueryOutput
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
)(implicit executionContext: ExecutionContext) {

  import Elastic4sAutoDerivation._

  def upsertMetadata(
    transferKey: TransferGvcfV1Key,
    transferMetadata: TransferGvcfV1Metadata
  ): Future[UpsertId] = {
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

  def querySchema(): Future[Json] = Future(GvcfIndex.jsonSchema)
}

object GvcfService {
  private[service] val v1DocumentConverter =
    AutoElasticsearchDocumentMapper[
      TransferGvcfV1Key,
      TransferGvcfV1Metadata,
      DocumentGvcf
    ]

  val v1QueryConverter: ElasticsearchQueryMapper[TransferGvcfV1QueryInput,
                                                 TransferGvcfV1QueryOutput,
                                                 DocumentGvcf] =
    AutoElasticsearchQueryMapper[
      TransferGvcfV1QueryInput,
      TransferGvcfV1QueryOutput,
      DocumentGvcf
    ]
}
