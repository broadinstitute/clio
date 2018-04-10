package org.broadinstitute.clio.server.service

import akka.NotUsed
import akka.stream.scaladsl.Source
import gnieh.diffson.circe._
import io.circe.Json
import io.circe.syntax._
import org.broadinstitute.clio.server.dataaccess.{PersistenceDAO, SearchDAO}
import org.broadinstitute.clio.server.dataaccess.elasticsearch._
import org.broadinstitute.clio.server.exceptions.UpsertValidationException
import org.broadinstitute.clio.transfer.model.ApiConstants._
import org.broadinstitute.clio.transfer.model._
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.DocumentStatus.Normal
import org.broadinstitute.clio.util.model.UpsertId

import scala.concurrent.ExecutionContext

abstract class IndexService[CI <: ClioIndex](
  persistenceDAO: PersistenceDAO,
  searchDAO: SearchDAO,
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
    // query by key to see if the document already exists
    if (!force) {
      queryMetadataForKey(indexKey)
        .fold[Either[Exception, clioIndex.MetadataType]](
          // If the query doesn't return a document and the document status is null then set it to the default value.
          Right(
            setDefaultDocumentStatus(metadata)
          )
        ) { (_, existingMetadataJson) =>
          // validation will check to make sure no fields are being overwritten
          validateUpsert(existingMetadataJson, metadata)
        }
        .flatMapConcat[UpsertId, NotUsed] {
          case Right(updatedMetadata) =>
            upsertToStream(indexKey, updatedMetadata)
          case Left(rejection) => Source.failed(rejection)
        }
    } else {
      upsertToStream(indexKey, metadata)
    }
  }

  /**
    * Update-or-insert (upsert) metadata for a given key.
    *
    * @param indexKey         The DTO for the key
    * @param updatedMetadata  The DTO for the metadata.
    * @return the ID for this upsert
    */
  private def upsertToStream(
    indexKey: clioIndex.KeyType,
    updatedMetadata: clioIndex.MetadataType
  ) = {
    val document = documentConverter.document(indexKey, updatedMetadata)
    val futureUpsertId = for {
      _ <- persistenceDAO.writeUpdate(document, elasticsearchIndex)
      _ <- searchDAO.updateMetadata(document)(elasticsearchIndex)
    } yield {
      ElasticsearchIndex.getUpsertId(document)
    }
    Source.fromFuture(futureUpsertId)
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
    rawQuery(queryConverter.buildQuery(input)(elasticsearchIndex))
  }

  /**
    * Run a raw Json query.
    *
    * @param inputJson  The json string of the query.
    * @return           The result of the query.
    */
  def rawQuery(inputJson: String): Source[Json, NotUsed] = {
    searchDAO
      .rawQuery(inputJson)(elasticsearchIndex)
      .map(queryConverter.toQueryOutput)
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

    rawQuery(keyQueryConverter.buildQuery(indexKey)(elasticsearchIndex))
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
