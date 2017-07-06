package org.broadinstitute.clio.server.dataaccess

import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.cluster.ClusterHealthResponse
import com.sksamuel.elastic4s.mappings.dynamictemplate.DynamicMapping
import com.typesafe.scalalogging.StrictLogging
import org.apache.http.HttpHost
import org.broadinstitute.clio.model.ElasticsearchStatusInfo
import org.broadinstitute.clio.server.ClioServerConfig
import org.broadinstitute.clio.server.ClioServerConfig.Elasticsearch.ElasticsearchHttpHost
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.elasticsearch.client.RestClient

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class HttpElasticsearchDAO private[dataaccess] (
  httpHosts: Seq[HttpHost]
)(implicit executionContext: ExecutionContext)
    extends SearchDAO
    with StrictLogging {

  private[dataaccess] val httpClient = {
    val restClient = RestClient.builder(httpHosts: _*).build()
    HttpClient.fromRestClient(restClient)
  }

  override def getClusterStatus: Future[ElasticsearchStatusInfo] = {
    getClusterHealth transform {
      case Success(health) =>
        Success(
          ElasticsearchStatusInfo(
            health.status,
            health.numberOfNodes,
            health.numberOfDataNodes
          )
        )
      case Failure(exception) =>
        logger.error(
          s"Error while getting Elasticsearch cluster status from $hostsString",
          exception
        )
        Success(HttpElasticsearchDAO.StatusError)
    }
  }

  override def isReady: Future[Boolean] = {
    getClusterHealth transform {
      case Success(health) =>
        if (ClioServerConfig.Elasticsearch.readinessColors.contains(
              health.status
            )) {
          Success(true)
        } else {
          logger.debug(
            s"health.status = ${health.status}, readyColors = ${ClioServerConfig.Elasticsearch.readinessColors}"
          )
          Success(false)
        }
      case Failure(exception) =>
        logger.debug(
          s"Error while getting Elasticsearch cluster status from $hostsString",
          exception
        )
        Success(false)
    }
  }

  override val readyRetries: Int =
    ClioServerConfig.Elasticsearch.readinessRetries

  override val readyPatience: FiniteDuration =
    ClioServerConfig.Elasticsearch.readinessPatience

  override def existsIndexType(index: ElasticsearchIndex[_]): Future[Boolean] = {
    val typesExistsDefinition = typesExist(index.indexName / index.indexType)
    httpClient.execute(typesExistsDefinition).map(_.exists)
  }

  override def createIndexType(index: ElasticsearchIndex[_]): Future[Unit] = {
    val createIndexDefinition = createIndex(index.indexName) mappings mapping(
      index.indexType
    )
    val replicatedIndexDefinition =
      if (ClioServerConfig.Elasticsearch.replicateIndices)
        createIndexDefinition
      else createIndexDefinition.replicas(0)
    httpClient
      .execute(replicatedIndexDefinition)
      .map(response => {
        if (response.acknowledged && response.shards_acknowledged)
          ()
        else {
          val message =
            s"""|Index creation was not acknowledged:
              |  Index: ${index.indexName}/${index.indexType}
              |  Acknowledged: ${response.acknowledged}
              |  Shards Acknowledged: ${response.shards_acknowledged}
              |""".stripMargin
          throw new RuntimeException(message)
        }
      })
  }

  override def updateFieldDefinitions(
    index: ElasticsearchIndex[_]
  ): Future[Unit] = {
    val indexAndType = index.indexName / index.indexType
    val fieldDefinitions = index.fields
    val putMappingDefinition = putMapping(indexAndType) dynamic DynamicMapping.False as (fieldDefinitions: _*)
    httpClient
      .execute(putMappingDefinition)
      .map(response => {
        if (response.acknowledged)
          ()
        else {
          val message =
            s"""|Put mapping was not acknowledged:
                |  Index: ${index.indexName}/${index.indexType}
                |  Acknowledged: ${response.acknowledged}
                |""".stripMargin
          throw new RuntimeException(message)
        }
      })
  }

  override def close(): Future[Unit] = {
    Future {
      closeClient()
    }
  }

  private[dataaccess] def closeClient(): Unit = {
    httpClient.close()
  }

  private def hostsString = httpHosts.mkString(", ")

  private[dataaccess] def getClusterHealth: Future[ClusterHealthResponse] = {
    val clusterHealthDefinition = clusterHealth()
    httpClient execute clusterHealthDefinition
  }
}

object HttpElasticsearchDAO extends StrictLogging {
  def apply()(implicit executionContext: ExecutionContext): SearchDAO = {
    val httpHosts = ClioServerConfig.Elasticsearch.httpHosts.map(toHttpHost)
    new HttpElasticsearchDAO(httpHosts)
  }

  private val StatusError = ElasticsearchStatusInfo("error", -1, -1)

  private def toHttpHost(elasticsearchHttpHost: ElasticsearchHttpHost) = {
    new HttpHost(
      elasticsearchHttpHost.hostname,
      elasticsearchHttpHost.port,
      elasticsearchHttpHost.scheme
    )
  }
}
