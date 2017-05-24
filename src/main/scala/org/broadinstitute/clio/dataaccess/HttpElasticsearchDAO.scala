package org.broadinstitute.clio.dataaccess

import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.cluster.ClusterHealthResponse
import com.typesafe.scalalogging.LazyLogging
import org.apache.http.HttpHost
import org.broadinstitute.clio.ClioConfig
import org.broadinstitute.clio.ClioConfig.Elasticsearch.ElasticsearchHttpHost
import org.broadinstitute.clio.model.ElasticsearchStatusInfo
import org.elasticsearch.client.RestClient

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class HttpElasticsearchDAO private[dataaccess](httpHosts: Seq[HttpHost])
                                              (implicit ec: ExecutionContext)
  extends ElasticsearchDAO with LazyLogging {

  private[dataaccess] val httpClient = {
    val restClient = RestClient.builder(httpHosts: _*).build()
    HttpClient.fromRestClient(restClient)
  }

  override def getClusterStatus: Future[ElasticsearchStatusInfo] = {
    getClusterHealth transform {
      case Success(health) =>
        Success(ElasticsearchStatusInfo(health.status, health.numberOfNodes, health.numberOfDataNodes))
      case Failure(exception) =>
        logger.error(s"Error while getting Elasticsearch cluster status from $hostsString", exception)
        Success(HttpElasticsearchDAO.StatusError)
    }
  }

  override def isReady: Future[Boolean] = {
    getClusterHealth transform {
      case Success(health) =>
        if (ClioConfig.Elasticsearch.readinessColors.contains(health.status)) {
          Success(true)
        } else {
          logger.debug(s"health.status = ${health.status}, readyColors = ${ClioConfig.Elasticsearch.readinessColors}")
          Success(false)
        }
      case Failure(exception) =>
        logger.debug(s"Error while getting Elasticsearch cluster status from $hostsString", exception)
        Success(false)
    }
  }

  override val readyRetries: Int = ClioConfig.Elasticsearch.readinessRetries

  override val readyPatience: FiniteDuration = ClioConfig.Elasticsearch.readinessPatience

  override def close(): Future[Unit] = {
    Future {
      closeClient()
    }
  }

  private[dataaccess] def closeClient(): Unit = {
    httpClient.close()
  }

  private def hostsString = httpHosts.mkString(", ")

  private def getClusterHealth: Future[ClusterHealthResponse] = {
    val clusterHealthDefinition = clusterHealth()
    httpClient execute clusterHealthDefinition
  }
}

object HttpElasticsearchDAO extends LazyLogging {
  val StatusError = ElasticsearchStatusInfo("error", -1, -1)

  def apply()(implicit ec: ExecutionContext): ElasticsearchDAO = {
    val httpHosts = ClioConfig.Elasticsearch.httpHosts.map(toHttpHost)
    new HttpElasticsearchDAO(httpHosts)
  }

  private def toHttpHost(elasticsearchHttpHost: ElasticsearchHttpHost) = {
    new HttpHost(elasticsearchHttpHost.hostname, elasticsearchHttpHost.port, elasticsearchHttpHost.scheme)
  }
}
