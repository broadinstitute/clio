package org.broadinstitute.clio.server.service

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import io.circe.syntax._
import io.circe.{Json, Printer}
import org.broadinstitute.clio.server.dataaccess.elasticsearch._
import org.broadinstitute.clio.server.exceptions.UpsertValidationException
import org.broadinstitute.clio.transfer.model.ClioIndex
import org.broadinstitute.clio.util.generic.{
  CaseClassMapper,
  CirceEquivalentCamelCaseLexer
}
import org.broadinstitute.clio.util.model.DocumentStatus.Normal
import org.broadinstitute.clio.util.model.{DocumentStatus, UpsertId}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

abstract class IndexService[CI <: ClioIndex](
  persistenceService: PersistenceService,
  searchService: SearchService,
  elasticsearchIndex: ElasticsearchIndex[CI],
  val clioIndex: CI
)(
  implicit
  executionContext: ExecutionContext
) {

  import clioIndex.implicits._

  private[service] val documentConverter =
    ElasticsearchDocumentMapper[
      clioIndex.KeyType,
      clioIndex.MetadataType
    ]

  private[service] val queryConverter =
    ElasticsearchQueryMapper[
      clioIndex.QueryInputType
    ]

  def upsertMetadata(
    indexKey: clioIndex.KeyType,
    metadata: clioIndex.MetadataType,
    force: Boolean = false
  )(implicit materializer: Materializer): Future[UpsertId] = {

    validateUpsert(indexKey, metadata, force) match {
      case Right(updatedMetadata) =>
        persistenceService
          .upsertMetadata(
            indexKey,
            updatedMetadata,
            documentConverter,
            elasticsearchIndex
          )
      case Left(rejection) => Future.failed(rejection)
    }
  }

  def queryMetadata(
    input: clioIndex.QueryInputType
  ): Source[Json, NotUsed] = {
    val newInput =
      input.withDocumentStatus(Option(DocumentStatus.Normal))
    queryAllMetadata(newInput)
  }

  def queryAllMetadata(
    input: clioIndex.QueryInputType
  ): Source[Json, NotUsed] = {
    searchService.queryMetadata(
      input,
      queryConverter
    )(
      elasticsearchIndex
    )
  }
  private val keysToDrop =
    Set(
      ElasticsearchIndex.UpsertIdElasticsearchName,
      ElasticsearchIndex.EntityIdElasticsearchName,
    )

  def validateUpsert(
    indexKey: clioIndex.KeyType,
    metadata: clioIndex.MetadataType,
    force: Boolean
  )(
    implicit materializer: Materializer
  ): Either[Exception, clioIndex.MetadataType] = {

    // drop housekeeping and key fields (evaluate only metadata)
    val dropKeys = keysToDrop ++ indexKey.getClass.getDeclaredFields
      .map(field => ElasticsearchUtil.toElasticsearchName(field.getName))

    val storedDocsForKey = Await
      .result(
        searchService
          .simpleStringQueryMetadata(indexKey.asJson.pretty(Printer.noSpaces))(
            elasticsearchIndex
          )
          .runWith(Sink.seq),
        1.second
      )
      .map(
        json =>
          json.mapObject(_.filterKeys({ key =>
            !dropKeys.contains(key)
          }))
      )

    if (storedDocsForKey.size > 1) {
      Left(
        UpsertValidationException(
          s"Key based query returned more than one entry. $indexKey"
        )
      )
    }

    if (storedDocsForKey.isEmpty) {
      // if there are no documents stored set document status to either what was passed in or Normal (default)
      Right(
        metadata.documentStatus
          .fold(metadata.withDocumentStatus(Some(Normal)))(_ => metadata)
      )
    } else {
      // validate that we will not overwrite any data?
      val storedDoc = storedDocsForKey.head.as[clioIndex.MetadataType].toOption

      val updatedMetadata = checkStoredDocumentStatus(metadata, storedDoc)
      val differences = storedDoc.fold(Iterable.empty[(String, Any, Any)]) {
        diffMetadata(_, updatedMetadata)
      }
      if (!force && differences.nonEmpty) {
        val diffs = differences.map {
          case (field, newVal, oldVal) =>
            s"Field: $field, Old value: $oldVal, New value: $newVal"
        }.mkString("\n")

        Left(
          UpsertValidationException(
            s"""Adding this document will overwrite the following existing metadata:
$diffs

Use 'force=true' to overwrite the existing data.""".stripMargin
          )
        )
      } else { Right(updatedMetadata) }
    }
  }

  def checkStoredDocumentStatus(
    metadata: clioIndex.MetadataType,
    storedMetadata: Option[clioIndex.MetadataType]
  ): clioIndex.MetadataType = {
    val updatedMetadata = storedMetadata.fold(
      // if there is no metadata currently stored for this key then set document status to Normal
      metadata.withDocumentStatus(Some(DocumentStatus.Normal))
    )(
      meta =>
        meta.documentStatus.fold(
          // if there is metadata stored for this key but no document status (shouldn't happen) set it to Normal
          metadata.withDocumentStatus(Some(Normal))
        )(
          // otherwise use the metadata passed in
          _ => metadata
      )
    )

    updatedMetadata
  }

  private def diffMetadata(
    existingMetadata: clioIndex.MetadataType,
    newMetadata: clioIndex.MetadataType
  ): Iterable[(String, Any, Any)] = {
    val mapper = new CaseClassMapper[clioIndex.MetadataType]

    val existingMetadataValues =
      mapper
        .vals(existingMetadata)
        .asInstanceOf[Map[String, Option[Any]]]
        .filterNot(_._2.isEmpty)

    val newMetadataValues =
      mapper
        .vals(newMetadata)
        .asInstanceOf[Map[String, Option[Any]]]
        .filterNot(_._2.isEmpty)

    val differentFields =
      newMetadataValues.keySet
        .intersect(existingMetadataValues.keySet)
        .filterNot { field =>
          existingMetadataValues(field).equals(newMetadataValues(field))
        }

    differentFields.map { key =>
      import s_mach.string._
      (
        key.toSnakeCase(CirceEquivalentCamelCaseLexer),
        newMetadataValues(key).get,
        existingMetadataValues(key).get
      )
    }
  }
}
