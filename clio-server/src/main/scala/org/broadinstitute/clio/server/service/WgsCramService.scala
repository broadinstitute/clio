package org.broadinstitute.clio.server.service

import com.sksamuel.elastic4s.circe._
import io.circe.Json
import org.broadinstitute.clio.server.dataaccess.elasticsearch._
import org.broadinstitute.clio.transfer.model.WgsCramIndex
import org.broadinstitute.clio.transfer.model.wgscram.{
  TransferWgsCramV1Key,
  TransferWgsCramV1Metadata,
  TransferWgsCramV1QueryInput,
  TransferWgsCramV1QueryOutput
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
)(implicit executionContext: ExecutionContext) {

  import Elastic4sAutoDerivation._

  def upsertMetadata(
    transferKey: TransferWgsCramV1Key,
    transferMetadata: TransferWgsCramV1Metadata
  ): Future[UpsertId] = {
    val updatedTransferMetadata = transferMetadata.copy(
      documentStatus =
        transferMetadata.documentStatus.orElse(Some(DocumentStatus.Normal))
    )

    persistenceService
      .upsertMetadata(
        transferKey,
        updatedTransferMetadata,
        ElasticsearchIndex.WgsCram,
        WgsCramService.v1DocumentConverter
      )
  }

  def queryMetadata(
    transferInput: TransferWgsCramV1QueryInput
  ): Future[Seq[TransferWgsCramV1QueryOutput]] = {
    val transferInputNew =
      transferInput.copy(documentStatus = Option(DocumentStatus.Normal))
    queryAllMetadata(transferInputNew)
  }

  def queryAllMetadata(
    transferInput: TransferWgsCramV1QueryInput
  ): Future[Seq[TransferWgsCramV1QueryOutput]] = {
    searchService.queryMetadata(
      transferInput,
      ElasticsearchIndex.WgsCram,
      WgsCramService.v1QueryConverter
    )
  }

  def querySchema(): Future[Json] = Future(WgsCramIndex.jsonSchema)
}

object WgsCramService {
  private[service] val v1DocumentConverter =
    AutoElasticsearchDocumentMapper[
      TransferWgsCramV1Key,
      TransferWgsCramV1Metadata,
      DocumentWgsCram
    ]

  val v1QueryConverter: ElasticsearchQueryMapper[TransferWgsCramV1QueryInput,
                                                 TransferWgsCramV1QueryOutput,
                                                 DocumentWgsCram] =
    AutoElasticsearchQueryMapper[
      TransferWgsCramV1QueryInput,
      TransferWgsCramV1QueryOutput,
      DocumentWgsCram
    ]
}