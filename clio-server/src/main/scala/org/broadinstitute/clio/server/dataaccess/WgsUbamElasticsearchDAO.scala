package org.broadinstitute.clio.server.dataaccess

import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ElasticsearchDocumentMapper,
  ElasticsearchIndex,
  ElasticsearchQueryMapper
}
import org.broadinstitute.clio.server.dataaccess.elasticsearch.Elastic4sAutoDerivation._
import org.broadinstitute.clio.server.model._

import com.sksamuel.elastic4s.circe._

import scala.concurrent.{ExecutionContext, Future}

trait WgsUbamElasticsearchDAO extends SearchDAO {
  this: HttpElasticsearchDAO =>

  implicit val executionContext: ExecutionContext

  override def updateWgsUbamMetadata(
    key: ModelWgsUbamKey,
    metadata: ModelWgsUbamMetadata
  ): Future[Unit] = {
    updateMetadata(
      ElasticsearchIndex.WgsUbam,
      ElasticsearchDocumentMapper.WgsUbam,
      key,
      metadata
    )
  }

  override def queryWgsUbam(
    queryInput: ModelWgsUbamQueryInput
  ): Future[Seq[ModelWgsUbamQueryOutput]] = {
    searchDocuments(
      ElasticsearchIndex.WgsUbam,
      ElasticsearchQueryMapper.WgsUbam,
      queryInput
    )
  }
}
