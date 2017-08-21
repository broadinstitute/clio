package org.broadinstitute.clio.server.service

import io.circe.Json
import org.broadinstitute.clio.server.ClioApp
import org.broadinstitute.clio.server.dataaccess.SearchDAO
import org.broadinstitute.clio.server.model._
import org.broadinstitute.clio.transfer.model.{TransferReadGroupV1QueryInput, _}
import org.broadinstitute.clio.util.generic.SameFieldsTypeConverter
import org.broadinstitute.clio.util.model.DocumentStatus
import org.broadinstitute.clio.util.json.JsonSchemas

import scala.concurrent.{ExecutionContext, Future}

class ReadGroupService private (
  searchDAO: SearchDAO
)(implicit executionContext: ExecutionContext) {

  def upsertMetadata(
    transferKey: TransferReadGroupV1Key,
    transferMetadata: TransferReadGroupV1Metadata
  ): Future[Unit] = {
    val updatedTransferMetadata = transferMetadata.copy(
      documentStatus =
        Option(transferMetadata.documentStatus.getOrElse(DocumentStatus.Normal))
    )

    SearchService.upsertMetadata(
      transferKey,
      updatedTransferMetadata,
      ReadGroupService.ConverterV1Key,
      ReadGroupService.ConverterV1Metadata,
      searchDAO.updateReadGroupMetadata
    )
  }

  def queryMetadata(
    transferInput: TransferReadGroupV1QueryInput
  ): Future[Seq[TransferReadGroupV1QueryOutput]] = {
    val transferInputNew =
      transferInput.copy(documentStatus = Option(DocumentStatus.Normal))
    queryAllMetadata(transferInputNew)
//    SearchService.queryMetadata(
//      transferInputNew,
//      ReadGroupService.ConverterV1QueryInput,
//      ReadGroupService.ConverterV1QueryOutput,
//      searchDAO.queryReadGroup
//    )
  }

  def queryAllMetadata(
    transferInput: TransferReadGroupV1QueryInput
  ): Future[Seq[TransferReadGroupV1QueryOutput]] = {
    SearchService.queryMetadata(
      transferInput,
      ReadGroupService.ConverterV1QueryInput,
      ReadGroupService.ConverterV1QueryOutput,
      searchDAO.queryReadGroup
    )
  }

  def querySchema(): Future[Json] = Future(JsonSchemas.ReadGroup)
}

object ReadGroupService {
  def apply(
    app: ClioApp
  )(implicit executionContext: ExecutionContext): ReadGroupService = {
    new ReadGroupService(app.searchDAO)
  }

  private[service] val ConverterV1Key =
    SameFieldsTypeConverter[TransferReadGroupV1Key, ModelReadGroupKey]

  private[service] val ConverterV1Metadata =
    SameFieldsTypeConverter[TransferReadGroupV1Metadata, ModelReadGroupMetadata]

  private[service] val ConverterV1QueryInput =
    SameFieldsTypeConverter[
      TransferReadGroupV1QueryInput,
      ModelReadGroupQueryInput
    ]

  private[service] val ConverterV1QueryOutput =
    SameFieldsTypeConverter[
      ModelReadGroupQueryOutput,
      TransferReadGroupV1QueryOutput
    ]
}
