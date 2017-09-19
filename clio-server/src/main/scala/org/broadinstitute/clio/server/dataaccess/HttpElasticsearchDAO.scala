package org.broadinstitute.clio.server.dataaccess

import akka.actor.ActorSystem
import com.sksamuel.elastic4s.bulk.BulkCompatibleDefinition
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.cluster.ClusterHealthResponse
import com.sksamuel.elastic4s.http.search.SearchResponse
import com.sksamuel.elastic4s.mappings.dynamictemplate.DynamicMapping
import com.sksamuel.elastic4s.searches.queries.QueryDefinition
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

class HttpElasticsearchDAO private[dataaccess] (
  private[dataaccess] val httpHosts: Seq[HttpHost]
)(implicit
  val executionContext: ExecutionContext,
  val system: ActorSystem)
    extends SearchDAO
    with SystemElasticsearchDAO
    with StrictLogging {

  private[dataaccess] val httpClient = {
    val restClient = RestClient
      .builder(httpHosts: _*)
      // The default timeout is 100̈ms, which is too slow for query operations.
      .setRequestConfigCallback(
        _.setConnectionRequestTimeout(
          ClioServerConfig.Elasticsearch.httpRequestTimeout.toMicros.toInt
        )
      )
      .build()
    HttpClient.fromRestClient(restClient)
  }

  override def updateMetadata[D <: ClioDocument: Indexable](
    document: D,
    index: ElasticsearchIndex[D]
  ): Future[Unit] = {
    bulkUpdate(updatePartialDocument(index, document))
  }

  override def queryMetadata[D <: ClioDocument: HitReader](
    queryDefinition: QueryDefinition,
    index: ElasticsearchIndex[D]
  ): Future[Seq[D]] = {
    val searchDefinition = search(index.indexName / index.indexType)
      .scroll(HttpElasticsearchDAO.DocumentScrollKeepAlive)
      .size(HttpElasticsearchDAO.DocumentScrollSize)
      .sortByFieldAsc(HttpElasticsearchDAO.DocumentScrollSort)
      .query(queryDefinition)
    val searchResponse = httpClient execute searchDefinition
    searchResponse flatMap foldScroll(Seq.empty[D], searchDefinition.keepAlive)
  }

  /**
    * Given an elastic search index, return the most recent document for that index.
    *
    * Document ordering is determined by the "upsert ID", which must be indexed field.
    *
    * @param index the elasticsearch index
    * @tparam D the document type this index contains
    * @return the most recent document for this index, if any
    */
  def getMostRecentDocument[D <: ClioDocument: HitReader](
    index: ElasticsearchIndex[D]
  ): Future[D] = {
    val searchDefinition = search(index.indexName / index.indexType)
      .size(1)
      .sortByFieldDesc(ClioDocument.UpsertIdElasticSearchName)
    for {
      searchResponse <- httpClient execute searchDefinition
    } yield {
      searchResponse.to[D] match {
        case Seq(document) => document
        case _ =>
          throw new RuntimeException(
            s"Could not get most recent document for index ${index.indexName}, found ${searchResponse.hits.total} documents."
          )
      }
    }
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

  private[dataaccess] def updatePartialDocument[D <: ClioDocument: Indexable](
    index: ElasticsearchIndex[D],
    document: D
  ): BulkCompatibleDefinition = {
    update(document.entityId) in index.indexName / index.indexType docAsUpsert document
  }

  private def foldScroll[I, D: HitReader](
    acc: Seq[D],
    keepAlive: Option[String]
  )(searchResponse: SearchResponse): Future[Seq[D]] = {
    searchResponse.scrollId match {
      case Some(scrollId) =>
        if (searchResponse.nonEmpty) {
          val scrollDefinition =
            searchScroll(scrollId).copy(keepAlive = keepAlive)
          val scrollResponse = httpClient execute scrollDefinition
          scrollResponse flatMap foldScroll(
            acc ++ searchResponse.to[D],
            keepAlive
          )
        } else {
          val clearResponse = httpClient execute clearScroll(scrollId)
          clearResponse transform (_ => Success(acc))
        }
      case None =>
        Future.successful(acc ++ searchResponse.to[D])
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
