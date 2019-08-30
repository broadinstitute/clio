package org.broadinstitute.clio.server.dataaccess

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import com.sksamuel.elastic4s.RefreshPolicy
import com.sksamuel.elastic4s.bulk.BulkCompatibleDefinition
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.cluster.ClusterHealthResponse
import com.sksamuel.elastic4s.mappings.dynamictemplate.DynamicMapping
import com.sksamuel.elastic4s.streams.ReactiveElastic._
import com.typesafe.scalalogging.StrictLogging
import io.circe.{Json, JsonObject}
import io.circe.syntax._
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
        ).setSocketTimeout(
          ClioServerConfig.Elasticsearch.socketTimeout.toMillis.toInt
        )
      )
      .build()
    HttpClient.fromRestClient(restClient)
  }

  override def updateMetadata(documents: Seq[Json])(
    implicit index: ElasticsearchIndex[_]
  ): Future[Unit] = bulkUpdate(documents.map(updatePartialDocument(index, _)))

  override def updateMetadata(document: Json)(
    implicit index: ElasticsearchIndex[_]
  ): Future[Unit] = updateMetadata(Seq(document))

  override def rawQuery(json: JsonObject)(
    implicit index: ElasticsearchIndex[_]
  ): Source[Json, NotUsed] = {
    val requestBody = HttpElasticsearchDAO.DefaultSearchParams
      .deepMerge(json.asJson)

    val searchDefinition = searchWithType(index.indexName / index.indexType)
      .scroll(HttpElasticsearchDAO.ElasticsearchScrollKeepAlive)
      .source(requestBody.pretty(ModelAutoDerivation.defaultPrinter))

    val responsePublisher = httpClient.publisher(
      searchDefinition,
      requestBody.hcursor
        .get[Long](HttpElasticsearchDAO.ElasticsearchSizeField)
        .getOrElse(Long.MaxValue)
    )

    Source
      .fromPublisher(responsePublisher)
      .map(_.to[Json])
      .alsoTo(
        Sink.foreach { json =>
          logger.debug(json.pretty(ModelAutoDerivation.defaultPrinter))
        }
      )
  }

  override def getMostRecentDocument(
    implicit index: ElasticsearchIndex[_]
  ): Future[Option[Json]] = {
    val searchDefinition = searchWithType(index.indexName / index.indexType)
      .size(1)
      .sortByFieldDesc(ElasticsearchIndex.UpsertIdElasticsearchName)
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
    val createIndexDefinition = createIndex(index.indexName)
      .mappings(mapping(index.indexType))
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
    definitions: Seq[BulkCompatibleDefinition]
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
    val id = index.getId(document)

    update(id)
      .in(index.indexName / index.indexType)
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

  private val ElasticsearchScrollKeepAlive = "1m"
  private val ElasticsearchSizeField = "size"

  private val DefaultSearchParams = Json.obj(
    "sort" -> Json.arr("_doc".asJson)
  )
}
