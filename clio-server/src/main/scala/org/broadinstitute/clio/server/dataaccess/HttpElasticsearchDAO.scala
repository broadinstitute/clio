package org.broadinstitute.clio.server.dataaccess

import akka.actor.ActorSystem
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.cluster.ClusterHealthResponse
import com.sksamuel.elastic4s.mappings.dynamictemplate.DynamicMapping
import com.typesafe.scalalogging.StrictLogging
import org.apache.http.HttpHost
import org.broadinstitute.clio.server.ClioServerConfig
import org.broadinstitute.clio.server.ClioServerConfig.Elasticsearch.ElasticsearchHttpHost
import org.broadinstitute.clio.server.dataaccess.elasticsearch._
import org.elasticsearch.client.RestClient

import scala.concurrent.{ExecutionContext, Future}

class HttpElasticsearchDAO private[dataaccess] (
  private[dataaccess] val httpHosts: Seq[HttpHost]
)(implicit
  val executionContext: ExecutionContext,
  val system: ActorSystem)
    extends SearchDAO
    with SystemElasticsearchDAO
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
    val typesExistsDefinition = typesExist(index.indexName / index.indexType)
    httpClient.execute(typesExistsDefinition).map(_.exists)
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
}
