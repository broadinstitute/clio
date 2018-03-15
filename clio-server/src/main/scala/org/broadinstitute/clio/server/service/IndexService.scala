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

  def querySchema(): Future[Json] = Future(clioIndex.jsonSchema)
}
