package org.broadinstitute.clio.server.service

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import gnieh.diffson.circe._
import io.circe.Json
import io.circe.syntax._
import org.broadinstitute.clio.server.dataaccess.elasticsearch._
import org.broadinstitute.clio.server.exceptions.UpsertValidationException
import org.broadinstitute.clio.transfer.model._
import org.broadinstitute.clio.util.model.DocumentStatus.Normal
import org.broadinstitute.clio.util.model.{DocumentStatus, UpsertId}

import scala.concurrent.{ExecutionContext, Future}

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
    // query by key to see if the document already exists
    queryMetadataForKey(indexKey)
      .runFold[Either[Exception, Json]](
        // If the query doesn't return a document and the document status is not set default to Normal.
        Right(
          metadata.documentStatus
            .fold(metadata.withDocumentStatus(Some(Normal)))(_ => metadata)
            .asJson
        )
      ) { (queryResult, _) =>
        // if we are not forcing the we want to run validation
        if (!force) {
          queryResult match {
            case Right(existingMetadata) =>
              // validation will check to make sure no fields are being overwritten
              validateUpsert(existingMetadata, metadata.asJson)
            case _ => queryResult
          }
        } else {
          queryResult
        }
      }
      .flatMap[UpsertId] {
        case Right(updateJson) =>
          updateJson.as[clioIndex.MetadataType].map { updateMetadata =>
            persistenceService.upsertMetadata(
              indexKey,
              updateMetadata,
              documentConverter,
              elasticsearchIndex
            )
          } match {
            case Right(upsertId) => upsertId
            case Left(failure)   => Future.failed(failure)
          }
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

  def validateUpsert(
    existingMetadata: Json,
    newMetadata: Json
  ): Either[Exception, Json] = {
    val differences = JsonDiff.diff(existingMetadata, newMetadata, remember = true)

    if (differences.ops.nonEmpty) {
      Right(newMetadata)
    } else {
      val diffs: String = differences.ops.map { op =>
        op.toString
      }.mkString("\n")
      Left(
        UpsertValidationException(
          s"""Adding this document will overwrite the following existing metadata:
               | $diffs. Use 'force=true' to overwrite the existing data.""".stripMargin
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
      .simpleStringQueryMetadata(indexKey.asJson.toString())(
        elasticsearchIndex,
        queryConverter
      )
      .fold[Either[Exception, Option[Json]]](Right(None)) { (acc, storedDocs) =>
        acc match {
          case Right(None) =>
            Right(Some(storedDocs.mapObject(_.filterKeys(!fieldsToDrop.contains(_)))))
          case Right(Some(_)) =>
            Left(
              UpsertValidationException(
                s"""Got > 1 ${clioIndex.name}s from Clio for key:
                 |${keyJson.toString}""".stripMargin
              )
            )
          case _ => acc
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
