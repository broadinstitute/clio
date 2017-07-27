package org.broadinstitute.clio.server.dataaccess

import com.sksamuel.elastic4s.circe._
import org.broadinstitute.clio.server.dataaccess.elasticsearch.Elastic4sAutoDerivation._
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ElasticsearchDocumentMapper,
  ElasticsearchIndex,
  ElasticsearchQueryMapper
}
import org.broadinstitute.clio.server.model._

import scala.concurrent.{ExecutionContext, Future}

trait ReadGroupElasticsearchDAO extends SearchDAO {
  this: HttpElasticsearchDAO =>

  implicit val executionContext: ExecutionContext

  override def updateReadGroupMetadata(
    key: ModelReadGroupKey,
    metadata: ModelReadGroupMetadata
  ): Future[Unit] = {
    updateMetadata(
      ElasticsearchIndex.ReadGroup,
      ElasticsearchDocumentMapper.ReadGroup,
      key,
      metadata
    )
  }

  override def queryReadGroup(
    queryInput: ModelReadGroupQueryInput
  ): Future[Seq[ModelReadGroupQueryOutput]] = {
    searchDocuments(
      ElasticsearchIndex.ReadGroup,
      ElasticsearchQueryMapper.ReadGroup,
      queryInput
    )
  }
}
