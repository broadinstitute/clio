package org.broadinstitute.clio.server.dataaccess

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import com.sksamuel.elastic4s.RefreshPolicy
import com.sksamuel.elastic4s.bulk.BulkCompatibleDefinition
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.cluster.ClusterHealthResponse
import com.sksamuel.elastic4s.mappings.dynamictemplate.DynamicMapping
import com.sksamuel.elastic4s.searches.queries.QueryDefinition
import com.sksamuel.elastic4s.streams.ReactiveElastic._
import com.typesafe.scalalogging.StrictLogging
import io.circe.Json
import org.apache.http.HttpHost
import org.broadinstitute.clio.server.ClioServerConfig
import org.broadinstitute.clio.server.ClioServerConfig.Elasticsearch.ElasticsearchHttpHost
import org.broadinstitute.clio.server.dataaccess.elasticsearch._
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.elasticsearch.client.RestClient

import scala.concurrent.{ExecutionContext, Future}

class HttpElasticsearchDAO private[dataaccess] (
  private[dataaccess] val httpHosts: Seq[HttpHost]
)(implicit val system: ActorSystem)
    extends SearchDAO
    with SystemElasticsearchDAO
    with StrictLogging {

  import ElasticsearchUtil.HttpClientOps
  import com.sksamuel.elastic4s.circe._

  implicit val ec: ExecutionContext = system.dispatcher

  private[dataaccess] val httpClient = {
    val restClient = RestClient
      .builder(httpHosts: _*)
      // The default timeout is 100̈ms, which is too slow for query operations.
      .setRequestConfigCallback(
        _.setConnectionRequestTimeout(
          ClioServerConfig.Elasticsearch.httpRequestTimeout.toMillis.toInt
        )
      )
      .build()
    HttpClient.fromRestClient(restClient)
  }

  override def updateMetadata(document: Json)(
    implicit index: ElasticsearchIndex[_]
  ): Future[Unit] = bulkUpdate(updatePartialDocument(index, document))

  override def queryMetadata[D <: ClioDocument](queryDefinition: QueryDefinition)(
    implicit index: ElasticsearchIndex[D]
  ): Source[D, NotUsed] = {
    import index.decoder

    val searchDefinition = searchWithType(index.indexName / index.indexType)
      .scroll(HttpElasticsearchDAO.DocumentScrollKeepAlive)
      .size(HttpElasticsearchDAO.DocumentScrollSize)
      .sortByFieldAsc(HttpElasticsearchDAO.DocumentScrollSort)
      .query(queryDefinition)

    val responsePublisher = httpClient.publisher(searchDefinition)
    Source.fromPublisher(responsePublisher).map(_.to[Json].as[D].fold(throw _, identity))
  }

  override def getMostRecentDocument(
    implicit index: ElasticsearchIndex[_]
  ): Future[Option[Json]] = {
    val searchDefinition = searchWithType(index.indexName / index.indexType)
      .size(1)
      .sortByFieldDesc(ClioDocument.UpsertIdElasticSearchName)
    httpClient
      .executeAndUnpack(searchDefinition)
      .map(_.to[Json].headOption)
  }

  private[dataaccess] def getClusterHealth: Future[ClusterHealthResponse] = {
    val clusterHealthDefinition = clusterHealth()
    httpClient.executeAndUnpack(clusterHealthDefinition)
  }

  private[dataaccess] def closeClient(): Unit = {
    httpClient.close()
  }

  private[dataaccess] def existsIndexType(
    index: ElasticsearchIndex[_]
  ): Future[Boolean] = {
    val indexExistsDefinition = indexExists(index.indexName)
    httpClient.executeAndUnpack(indexExistsDefinition).map(_.exists)
  }

  private[dataaccess] def createIndexType(
    index: ElasticsearchIndex[_]
  ): Future[Unit] = {
    val createIndexDefinition = createIndex(index.indexName).mappings(
      mapping(index.indexType)
    )
    val replicatedIndexDefinition =
      if (ClioServerConfig.Elasticsearch.replicateIndices)
        createIndexDefinition
      else createIndexDefinition.replicas(0)
    httpClient.executeAndUnpack(replicatedIndexDefinition).map { response =>
      if (!response.acknowledged || !response.shards_acknowledged) {
        throw new RuntimeException(
          s"""|Bad response:
              |$replicatedIndexDefinition
              |$response""".stripMargin
        )
      }
      ()
    }
  }

  private[dataaccess] def updateFieldDefinitions(
    index: ElasticsearchIndex[_]
  ): Future[Unit] = {
    val putMappingDefinition =
      putMapping(index.indexName / index.indexType)
        .dynamic(DynamicMapping.False)
        .as(index.fields: _*)
    httpClient.executeAndUnpack(putMappingDefinition).map { response =>
      if (!response.acknowledged) {
        throw new RuntimeException(
          s"""|Bad response:
              |$putMappingDefinition
              |$response""".stripMargin
        )
      }
      ()
    }
  }

  private[dataaccess] def bulkUpdate(
    definitions: BulkCompatibleDefinition*
  ): Future[Unit] = {
    val bulkDefinition = bulk(definitions).refresh(RefreshPolicy.WAIT_UNTIL)
    httpClient.executeAndUnpack(bulkDefinition).map { response =>
      if (response.errors || response.hasFailures) {
        throw new RuntimeException(
          s"""|Bad response:
              |$bulkDefinition
              |$response""".stripMargin
        )
      }
      ()
    }
  }

  private[dataaccess] def updatePartialDocument(
    index: ElasticsearchIndex[_],
    document: Json
  ): BulkCompatibleDefinition = {
    update(
      document.hcursor
        .get[String](ClioDocument.EntityIdElasticSearchName)
        .fold(throw _, identity)
    ).in(index.indexName / index.indexType)
      .docAsUpsert(document.pretty(ModelAutoDerivation.defaultPrinter))
  }
}

object HttpElasticsearchDAO extends StrictLogging {

  def apply()(implicit system: ActorSystem): SearchDAO = {
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
