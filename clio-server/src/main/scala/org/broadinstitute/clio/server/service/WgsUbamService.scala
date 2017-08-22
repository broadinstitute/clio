package org.broadinstitute.clio.server.service

import com.eaio.uuid.UUID
import io.circe.Json
import org.broadinstitute.clio.server.ClioApp
import org.broadinstitute.clio.server.dataaccess.SearchDAO
import org.broadinstitute.clio.server.model._
import org.broadinstitute.clio.transfer.model._
import org.broadinstitute.clio.util.generic.SameFieldsTypeConverter
import org.broadinstitute.clio.util.model.DocumentStatus
import org.broadinstitute.clio.util.json.JsonSchemas

import scala.concurrent.{ExecutionContext, Future}

class WgsUbamService private (
  searchDAO: SearchDAO
)(implicit executionContext: ExecutionContext) {

  def upsertMetadata(
    transferKey: TransferWgsUbamV1Key,
    transferMetadata: TransferWgsUbamV1Metadata
  ): Future[Long] = {
    val clioId = new UUID().getClockSeqAndNode
    val updatedTransferMetadata = transferMetadata.copy(
      documentStatus = Option(
        transferMetadata.documentStatus.getOrElse(DocumentStatus.Normal)
      ),
      clioId = Option(clioId)
    )
    SearchService
      .upsertMetadata(
        transferKey,
        updatedTransferMetadata,
        WgsUbamService.ConverterV1Key,
        WgsUbamService.ConverterV1Metadata,
        searchDAO.updateWgsUbamMetadata
      )
      .map(_ => clioId)
  }

  def queryMetadata(
    transferInput: TransferWgsUbamV1QueryInput
  ): Future[Seq[TransferWgsUbamV1QueryOutput]] = {
    val transferInputNew =
      transferInput.copy(documentStatus = Option(DocumentStatus.Normal))
    queryAllMetadata(transferInputNew)
  }

  def queryAllMetadata(
    transferInput: TransferWgsUbamV1QueryInput
  ): Future[Seq[TransferWgsUbamV1QueryOutput]] = {
    SearchService.queryMetadata(
      transferInput,
      WgsUbamService.ConverterV1QueryInput,
      WgsUbamService.ConverterV1QueryOutput,
      searchDAO.queryWgsUbam
    )
  }

  def querySchema(): Future[Json] = Future(JsonSchemas.WgsUbam)
}

object WgsUbamService {
  def apply(
    app: ClioApp
  )(implicit executionContext: ExecutionContext): WgsUbamService = {
    new WgsUbamService(app.searchDAO)
  }

  private[service] val ConverterV1Key =
    SameFieldsTypeConverter[TransferWgsUbamV1Key, ModelWgsUbamKey]

  private[service] val ConverterV1Metadata =
    SameFieldsTypeConverter[TransferWgsUbamV1Metadata, ModelWgsUbamMetadata]

  private[service] val ConverterV1QueryInput =
    SameFieldsTypeConverter[TransferWgsUbamV1QueryInput, ModelWgsUbamQueryInput]

  private[service] val ConverterV1QueryOutput =
    SameFieldsTypeConverter[
      ModelWgsUbamQueryOutput,
      TransferWgsUbamV1QueryOutput
    ]
}
