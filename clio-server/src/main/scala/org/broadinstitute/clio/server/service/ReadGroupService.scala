package org.broadinstitute.clio.server.service

import io.circe.Json
import org.broadinstitute.clio.server.ClioApp
import org.broadinstitute.clio.server.dataaccess.SearchDAO
import org.broadinstitute.clio.server.model._
import org.broadinstitute.clio.transfer.model._
import org.broadinstitute.clio.util.generic.{
  CaseClassTypeConverter,
  SameFieldsTypeConverter
}

import scala.concurrent.{ExecutionContext, Future}

class ReadGroupService private (
  searchDAO: SearchDAO
)(implicit executionContext: ExecutionContext) {

  def upsertMetadata(
    transferKey: TransferReadGroupV1Key,
    transferMetadata: TransferReadGroupV1Metadata
  ): Future[Unit] = {
    SearchService.upsertMetadata(
      transferKey,
      transferMetadata,
      ReadGroupService.ConverterV1Key,
      ReadGroupService.ConverterV1Metadata,
      searchDAO.updateReadGroupMetadata
    )
  }

  def upsertMetadataV2(
    transferKey: TransferReadGroupV2Key,
    transferMetadata: TransferReadGroupV1Metadata
  ): Future[Unit] = {
    SearchService.upsertMetadata(
      transferKey,
      transferMetadata,
      ReadGroupService.ConverterV2Key,
      ReadGroupService.ConverterV1Metadata,
      searchDAO.updateReadGroupMetadata
    )
  }

  def queryMetadata(
    transferInput: TransferReadGroupV1QueryInput
  ): Future[Seq[TransferReadGroupV1QueryOutput]] = {
    SearchService.queryMetadata(
      transferInput,
      ReadGroupService.ConverterV1QueryInput,
      ReadGroupService.ConverterV1QueryOutput,
      searchDAO.queryReadGroup
    )
  }

  def queryMetadataV2(
    transferInput: TransferReadGroupV2QueryInput
  ): Future[Seq[TransferReadGroupV2QueryOutput]] = {
    SearchService.queryMetadata(
      transferInput,
      ReadGroupService.ConverterV2QueryInput,
      ReadGroupService.ConverterV2QueryOutput,
      searchDAO.queryReadGroup
    )
  }

  def querySchema(): Future[Json] =
    SchemaService.readGroupSchema

  def querySchemaV2(): Future[Json] =
    SchemaService.readGroupSchemaV2
}

object ReadGroupService {
  def apply(
    app: ClioApp
  )(implicit executionContext: ExecutionContext): ReadGroupService = {
    new ReadGroupService(app.searchDAO)
  }

  private[service] val ConverterV1Key =
    CaseClassTypeConverter[TransferReadGroupV1Key, ModelReadGroupKey] {
      _ + TransferReadGroupLocation.unknown
    }

  private[service] val ConverterV1Metadata =
    SameFieldsTypeConverter[
      TransferReadGroupV1Metadata,
      ModelReadGroupMetadata
    ]

  private[service] val ConverterV2Key =
    SameFieldsTypeConverter[TransferReadGroupV2Key, ModelReadGroupKey]

  private[service] val ConverterV1QueryInput =
    CaseClassTypeConverter[
      TransferReadGroupV1QueryInput,
      ModelReadGroupQueryInput
    ] { _.filterKeys(_ != TransferReadGroupLocation.key) }

  private[service] val ConverterV1QueryOutput =
    CaseClassTypeConverter[
      ModelReadGroupQueryOutput,
      TransferReadGroupV1QueryOutput
    ] { _.filterKeys(_ != TransferReadGroupLocation.key) }

  private[service] val ConverterV2QueryInput =
    SameFieldsTypeConverter[
      TransferReadGroupV2QueryInput,
      ModelReadGroupQueryInput
    ]

  private[service] val ConverterV2QueryOutput =
    SameFieldsTypeConverter[
      ModelReadGroupQueryOutput,
      TransferReadGroupV2QueryOutput
    ]
}
