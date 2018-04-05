package org.broadinstitute.clio.server.service

import akka.NotUsed
import akka.stream.scaladsl.Source
import io.circe.Json
import org.broadinstitute.clio.server.dataaccess.elasticsearch._
import org.broadinstitute.clio.transfer.model.ClioIndex
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

  private val keysToDrop =
    Set(
      ElasticsearchIndex.UpsertIdElasticsearchName,
      ElasticsearchIndex.EntityIdElasticsearchName
    )

  def upsertMetadata(
    indexKey: clioIndex.KeyType,
    metadata: clioIndex.MetadataType
  ): Future[UpsertId] = {
    val updatedMetadata = metadata.withDocumentStatus(
      metadata.documentStatus.orElse(Some(DocumentStatus.Normal))
    )

    persistenceService
      .upsertMetadata(
        indexKey,
        updatedMetadata,
        documentConverter,
        elasticsearchIndex
      )
  }

  def queryMetadata(
    input: clioIndex.QueryInputType
  ): Source[Json, NotUsed] = {
    queryRaw(queryConverter.buildQuery(input))
  }

  def queryRaw(inputJson: String): Source[Json, NotUsed] = {
    searchService
      .rawQuery(inputJson)
      .map(toQueryOutput)
  }

  /**
    * Munges the query result document into the appropriate form for the query output.
    *
    * @param document A query result document.
    * @return The query output.
    */
  def toQueryOutput(document: Json): Json = {
    document.mapObject(_.filterKeys({ key =>
      !keysToDrop.contains(key)
    }))
  }
}
