package org.broadinstitute.clio.server.dataaccess

import akka.actor.ActorSystem
import com.sksamuel.elastic4s.bulk.BulkCompatibleDefinition
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.cluster.ClusterHealthResponse
import com.sksamuel.elastic4s.http.search.SearchResponse
import com.sksamuel.elastic4s.mappings.dynamictemplate.DynamicMapping
import com.sksamuel.elastic4s.{HitReader, Indexable}
import com.typesafe.scalalogging.StrictLogging
import org.apache.http.HttpHost
import org.broadinstitute.clio.server.ClioServerConfig
import org.broadinstitute.clio.server.ClioServerConfig.Elasticsearch.ElasticsearchHttpHost
import org.broadinstitute.clio.server.dataaccess.elasticsearch._

import org.elasticsearch.action.support.WriteRequest.RefreshPolicy
import org.elasticsearch.client.RestClient

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

import java.util.UUID

class HttpElasticsearchDAO private[dataaccess] (
  private[dataaccess] val httpHosts: Seq[HttpHost]
)(implicit
  val executionContext: ExecutionContext,
  val system: ActorSystem)
    extends SearchDAO
    with SystemElasticsearchDAO
    with WgsUbamElasticsearchDAO
    with StrictLogging {

  private[dataaccess] val httpClient = {
    val restClient = RestClient.builder(httpHosts: _*).build()
    HttpClient.fromRestClient(restClient)
  }

  private[dataaccess] def getClusterHealth: Future[ClusterHealthResponse] = {
    val clusterHealthDefinition = clusterHealth()
    httpClient execute clusterHealthDefinition
  }

  private[dataaccess] def closeClient(): Unit = {
    httpClient.close()
  }

  private[dataaccess] def existsIndexType(
    index: ElasticsearchIndex[_]
  ): Future[Boolean] = {
    val indexExistsDefinition = indexExists(index.indexName)
    httpClient.execute(indexExistsDefinition).map(_.exists)
  }

  private[dataaccess] def createIndexType(
    index: ElasticsearchIndex[_]
  ): Future[Unit] = {
    val createIndexDefinition = createIndex(index.indexName) mappings mapping(
      index.indexType
    )
    val replicatedIndexDefinition =
      if (ClioServerConfig.Elasticsearch.replicateIndices)
        createIndexDefinition
      else createIndexDefinition.replicas(0)
    httpClient.execute(replicatedIndexDefinition) map { response =>
      if (!response.acknowledged || !response.shards_acknowledged)
        throw new RuntimeException(s"""|Bad response:
                                       |$replicatedIndexDefinition
                                       |$response""".stripMargin)
      ()
    }
  }

  private[dataaccess] def updateFieldDefinitions(
    index: ElasticsearchIndex[_]
  ): Future[Unit] = {
    val putMappingDefinition =
      putMapping(index.indexName / index.indexType) dynamic DynamicMapping.False as (index.fields: _*)
    httpClient.execute(putMappingDefinition) map { response =>
      if (!response.acknowledged)
        throw new RuntimeException(s"""|Bad response:
                                       |$putMappingDefinition
                                       |$response""".stripMargin)
      ()
    }
  }

  private[dataaccess] def bulkUpdate(
    definitions: BulkCompatibleDefinition*
  ): Future[Unit] = {
    val bulkDefinition = bulk(definitions) refresh RefreshPolicy.WAIT_UNTIL
    httpClient.execute(bulkDefinition) map { response =>
      if (response.errors || response.hasFailures)
        throw new RuntimeException(s"""|Bad response:
              |$bulkDefinition
              |$response""".stripMargin)
      ()
    }
  }

  private[dataaccess] def updatePartialDocument[A: Indexable](
    index: ElasticsearchIndex[A],
    id: Any,
    document: A
  ): BulkCompatibleDefinition = {
    update(id) in index.indexName / index.indexType docAsUpsert document
  }

  private[dataaccess] def updateMetadata[MK, MM, D <: ClioDocument: Indexable](
    indexDocument: ElasticsearchIndex[D],
    mapper: ElasticsearchDocumentMapper[MK, MM, D],
    key: MK,
    metadata: MM
  ): Future[UUID] = {
    val id = mapper.id(key)
    val empty = mapper.empty(key)
    val document = mapper.withMetadata(empty, metadata)
    bulkUpdate(updatePartialDocument(indexDocument, id, document)).map { _ =>
      document.clioId
    }
  }

  private[dataaccess] def searchDocuments[I, O, D: HitReader](
    index: ElasticsearchIndex[D],
    queryBuilder: ElasticsearchQueryMapper[I, O, D],
    queryInput: I
  ): Future[Seq[O]] = {
    if (queryBuilder.isEmpty(queryInput)) {
      Future.successful(Seq.empty)
    } else {
      val searchDefinition = search(index.indexName / index.indexType)
        .scroll(HttpElasticsearchDAO.DocumentScrollKeepAlive)
        .size(HttpElasticsearchDAO.DocumentScrollSize)
        .sortByFieldAsc(HttpElasticsearchDAO.DocumentScrollSort)
        .query(queryBuilder.buildQuery(queryInput))
      val searchResponse = httpClient execute searchDefinition
      searchResponse flatMap foldScroll(
        Seq.empty[O],
        queryBuilder,
        searchDefinition.keepAlive
      )
    }
  }

  private def foldScroll[I, O, D: HitReader](
    acc: Seq[O],
    queryBuilder: ElasticsearchQueryMapper[I, O, D],
    keepAlive: Option[String]
  )(searchResponse: SearchResponse): Future[Seq[O]] = {
    searchResponse.scrollId match {
      case Some(scrollId) =>
        if (searchResponse.nonEmpty) {
          val scrollDefinition =
            searchScroll(scrollId).copy(keepAlive = keepAlive)
          val scrollResponse = httpClient execute scrollDefinition
          scrollResponse flatMap foldScroll(
            acc ++ queryBuilder.toQueryOutputs(searchResponse),
            queryBuilder,
            keepAlive
          )
        } else {
          val clearResponse = httpClient execute clearScroll(scrollId)
          clearResponse transform (_ => Success(acc))
        }
      case None =>
        Future.successful(acc ++ queryBuilder.toQueryOutputs(searchResponse))
    }
  }
}

object HttpElasticsearchDAO extends StrictLogging {
  def apply()(implicit executionContext: ExecutionContext,
              system: ActorSystem): SearchDAO = {
    val httpHosts = ClioServerConfig.Elasticsearch.httpHosts.map(toHttpHost)
    new HttpElasticsearchDAO(httpHosts)
  }

  private def toHttpHost(elasticsearchHttpHost: ElasticsearchHttpHost) = {
    new HttpHost(
      elasticsearchHttpHost.hostname,
      elasticsearchHttpHost.port,
      elasticsearchHttpHost.scheme
    )
  }

  private val DocumentScrollKeepAlive = "1m"
  private val DocumentScrollSize = 1024
  private val DocumentScrollSort = "_doc"
}
