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

  def upsertMetadata(
    indexKey: clioIndex.KeyType,
    metadata: clioIndex.MetadataType,
    force: Boolean = false
  ): Source[UpsertId, NotUsed] = {
    // query by key to see if the document already exists
    queryMetadataForKey(indexKey)
      .fold[Either[Exception, clioIndex.MetadataType]](
        // If the query doesn't return a document and the document status is not set default to Normal.
        Right(
          metadata.documentStatus
            .fold(metadata.withDocumentStatus(Some(Normal)))(_ => metadata)
        )
      ) { (_, existingMetadataJson) =>
        // if we are not forcing the we want to run validation
        if (!force) {
          // validation will check to make sure no fields are being overwritten
          validateUpsert(existingMetadataJson, metadata)
        } else {
          Right(metadata)
        }
      }
      .flatMapConcat[UpsertId, NotUsed] {
        case Right(updatedMetadata) =>
          persistenceService.upsertMetadata(
            indexKey,
            updatedMetadata,
            documentConverter,
            elasticsearchIndex
          )
        case Left(rejection) => Source.failed(rejection)
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

  def validateUpsert(
    existingMetadata: Json,
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
      .simpleStringQueryMetadata(indexKey.asJson)(
        elasticsearchIndex,
        queryConverter
      )
      .fold[Either[Exception, Option[Json]]](Right(None)) { (acc, storedDocs) =>
        acc.flatMap {
          case None =>
            Right(Some(storedDocs.mapObject(_.filterKeys(!fieldsToDrop.contains(_)))))
          case Some(_) =>
            Left(
              UpsertValidationException(
                s"""Got > 1 ${clioIndex.name}s from Clio for key:
                 |${indexKey.asJson
                     .pretty(ModelAutoDerivation.defaultPrinter)}""".stripMargin
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
