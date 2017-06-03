package org.broadinstitute.clio.dataaccess

import java.lang.{Boolean => JBoolean, Double => JDouble, Float => JFloat, Integer => JInteger, Long => JLong}
import java.time.OffsetDateTime

import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.cluster.ClusterHealthResponse
import com.sksamuel.elastic4s.mappings.FieldDefinition
import com.sksamuel.elastic4s.mappings.dynamictemplate.DynamicMapping
import com.typesafe.scalalogging.StrictLogging
import org.apache.http.HttpHost
import org.broadinstitute.clio.ClioConfig
import org.broadinstitute.clio.ClioConfig.Elasticsearch.ElasticsearchHttpHost
import org.broadinstitute.clio.model.{ElasticsearchField, ElasticsearchIndex, ElasticsearchStatusInfo}
import org.elasticsearch.client.RestClient

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class HttpElasticsearchDAO private[dataaccess](httpHosts: Seq[HttpHost])
                                              (implicit ec: ExecutionContext)
  extends ElasticsearchDAO with StrictLogging {

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

  override def existsIndexType(index: ElasticsearchIndex): Future[Boolean] = {
    val typesExistsDefinition = typesExist(index.indexName / index.indexType)
    httpClient.execute(typesExistsDefinition).map(_.exists)
  }

  override def createIndexType(index: ElasticsearchIndex, replicate: Boolean): Future[Unit] = {
    val createIndexDefinition = createIndex(index.indexName) mappings mapping(index.indexType)
    val replicatedIndexDefinition = if (replicate) createIndexDefinition else createIndexDefinition.replicas(0)
    httpClient.execute(replicatedIndexDefinition).map(response => {
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

  override def updateFieldDefinitions(index: ElasticsearchIndex): Future[Unit] = {
    val indexAndType = index.indexName / index.indexType
    val fieldDefinitions = index.fields map HttpElasticsearchDAO.fieldDefinition
    val putMappingDefinition = putMapping(indexAndType) dynamic DynamicMapping.False as (fieldDefinitions: _*)
    httpClient.execute(putMappingDefinition).map(response => {
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

  private def getClusterHealth: Future[ClusterHealthResponse] = {
    val clusterHealthDefinition = clusterHealth()
    httpClient execute clusterHealthDefinition
  }
}

object HttpElasticsearchDAO extends StrictLogging {
  val StatusError = ElasticsearchStatusInfo("error", -1, -1)

  def apply()(implicit ec: ExecutionContext): ElasticsearchDAO = {
    val httpHosts = ClioConfig.Elasticsearch.httpHosts.map(toHttpHost)
    new HttpElasticsearchDAO(httpHosts)
  }

  private def toHttpHost(elasticsearchHttpHost: ElasticsearchHttpHost) = {
    new HttpHost(elasticsearchHttpHost.hostname, elasticsearchHttpHost.port, elasticsearchHttpHost.scheme)
  }

  private def fieldDefinition(elasticsearchField: ElasticsearchField): FieldDefinition = {
    val fieldName = elasticsearchField.fieldName
    elasticsearchField.fieldType match {
      case clazz if clazz == classOf[Boolean] => booleanField(fieldName)
      case clazz if clazz == classOf[JBoolean] => booleanField(fieldName)
      case clazz if clazz == classOf[Int] => intField(fieldName)
      case clazz if clazz == classOf[JInteger] => intField(fieldName)
      case clazz if clazz == classOf[Long] => longField(fieldName)
      case clazz if clazz == classOf[JLong] => longField(fieldName)
      case clazz if clazz == classOf[Float] => floatField(fieldName)
      case clazz if clazz == classOf[JFloat] => floatField(fieldName)
      case clazz if clazz == classOf[Double] => doubleField(fieldName)
      case clazz if clazz == classOf[JDouble] => doubleField(fieldName)
      case clazz if clazz == classOf[String] => keywordField(fieldName)
      case clazz if clazz == classOf[OffsetDateTime] => dateField(fieldName)
      case clazz => throw new RuntimeException(s"No support for $fieldName: $clazz")
    }
  }
}
