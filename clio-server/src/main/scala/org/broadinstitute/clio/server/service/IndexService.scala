package org.broadinstitute.clio.server.service

import akka.NotUsed
import akka.stream.scaladsl.Source
import gnieh.diffson.circe._
import io.circe.Json
import io.circe.syntax._
import org.broadinstitute.clio.server.dataaccess.elasticsearch._
import org.broadinstitute.clio.server.exceptions.UpsertValidationException
import org.broadinstitute.clio.transfer.model.ApiConstants._
import org.broadinstitute.clio.transfer.model._
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.DocumentStatus.Normal
import org.broadinstitute.clio.util.model.{DocumentStatus, UpsertId}

import scala.concurrent.ExecutionContext

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
  private[service] val keyQueryConverter =
    ElasticsearchQueryMapper[clioIndex.KeyType]

  def upsertMetadata(
    indexKey: clioIndex.KeyType,
    metadata: clioIndex.MetadataType,
    force: Boolean = false
  ): Source[UpsertId, NotUsed] = {
    // query by key to see if the document already exists if force is false
    if (!force) {
      queryMetadataForKey(indexKey)
        .fold[Either[Exception, clioIndex.MetadataType]](
          // If the query doesn't return a document and the document status is null then set it to the default value.
          Right(
            setDefaultDocumentStatus(metadata)
          )
        ) { (_, existingMetadataJson) =>
          //if the incoming document status is not set then set it to the existing status
          existingMetadataJson
            .as[clioIndex.MetadataType]
            .flatMap { existing =>
              validateUpsert(
                existing,
                metadata.withDocumentStatus(existing.documentStatus)
              )
            }
        }
        .flatMapConcat[UpsertId, NotUsed] {
          case Right(updatedMetadata) =>
            upsertToStream(indexKey, updatedMetadata)
          case Left(rejection) => Source.failed(rejection)
        }
    } else {
      //if the incoming document status is not set then set it to the existing status
      metadata.documentStatus.fold[Source[UpsertId, NotUsed]]({
        queryMetadataForKey(indexKey).fold[Either[Exception, clioIndex.MetadataType]](
          // If the query doesn't return a document and the document status is null then set it to the default value.
          Right(
            setDefaultDocumentStatus(metadata)
          )
        ) { (_, existingMetadataJson) =>
          //if the incoming document status is not set then set it to the existing status
          existingMetadataJson
            .as[clioIndex.MetadataType]
            .flatMap { existing =>
              Right(metadata.withDocumentStatus(existing.documentStatus))
            }
        }
      }.flatMapConcat[UpsertId, NotUsed] {
        case Right(updatedMetadata) =>
          upsertToStream(indexKey, updatedMetadata)
        case Left(rejection) => Source.failed(rejection)
      }) { _ =>
        upsertToStream(indexKey, metadata)
      }
    }
  }

  private def upsertToStream(
    indexKey: clioIndex.KeyType,
    updatedMetadata: clioIndex.MetadataType
  ) = {
    Source.fromFuture(
      persistenceService.upsertMetadata(
        indexKey,
        updatedMetadata,
        documentConverter,
        elasticsearchIndex
      )
    )
  }

  private def setDefaultDocumentStatus(
    metadata: clioIndex.MetadataType
  ): clioIndex.MetadataType = {
    metadata.documentStatus
      .fold(metadata.withDocumentStatus(Some(Normal)))(_ => metadata)
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

  def validateUpsert(
    existingMetadata: clioIndex.MetadataType,
    newMetadata: clioIndex.MetadataType
  ): Either[Exception, clioIndex.MetadataType] = {
    val differences = JsonDiff.diff(existingMetadata, newMetadata, remember = true)
    /* Types of differences:
        Replace - the existing value is different from what we want to set. We ignore nulls. This will overwrite.
        Remove - the existing value doesn't exist in the new meta data. This will not overwrite.
        Add - the existing value doesn't exist. This will not overwrite.
        Move - effectively an Add and Remove. This will not overwrite.
        Copy - effectively an Add. This will not overwrite.
     */
    val replaceOperations: Seq[Replace] = differences.ops.flatMap {
      case replace: Replace => Some(replace).filter(_.value != Json.Null)
      case _                => None
    }

    if (replaceOperations.isEmpty) {
      Right(newMetadata)
    } else {
      val diffs: String = replaceOperations.map { replaceOp =>
        s"Field: ${replaceOp.path}, Old value: ${replaceOp.old.getOrElse("")}, New value: ${replaceOp.value}"
      }.mkString("\n")
      Left(
        UpsertValidationException(
          s"""Adding this document will overwrite the following existing metadata:
             |$diffs. Use '$forceString=true' to overwrite the existing data.""".stripMargin
        )
      )
    }
  }

  def queryMetadataForKey(
    indexKey: clioIndex.KeyType
  ): Source[Json, NotUsed] = {
    val keyJson = indexKey.asJsonObject
    val fieldsToDrop = keyJson.keys.toSeq

    searchService
      .queryMetadata(indexKey, keyQueryConverter)(
        elasticsearchIndex
      )
      .fold[Either[Exception, Option[Json]]](Right(None)) { (acc, storedDocs) =>
        acc.flatMap {
          case None =>
            Right(Some(storedDocs.mapObject(_.filterKeys(!fieldsToDrop.contains(_)))))
          case Some(_) =>
            val keyAsJson = indexKey.asJson.pretty(ModelAutoDerivation.defaultPrinter)
            Left(
              UpsertValidationException(
                s"Got > 1 ${clioIndex.name}s from Clio for key:$keyAsJson"
              )
            )
        }
      }
      .flatMapConcat {
        _.fold(
          Source.failed,
          _.fold(Source.empty[Json])(Source.single)
        )
      }
  }
}
