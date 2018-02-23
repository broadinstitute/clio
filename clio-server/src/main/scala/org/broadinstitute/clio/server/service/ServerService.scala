package org.broadinstitute.clio.server.service

import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.typesafe.scalalogging.StrictLogging
import io.circe.Json
import org.broadinstitute.clio.server.{ClioApp, ClioServerConfig}
import org.broadinstitute.clio.server.dataaccess.elasticsearch._
import org.broadinstitute.clio.server.dataaccess.{HttpServerDAO, PersistenceDAO, SearchDAO, ServerStatusDAO}
import org.broadinstitute.clio.status.model.ClioStatus
import org.broadinstitute.clio.transfer.model.gvcf.{TransferGvcfV1Key, TransferGvcfV1Metadata}
import org.broadinstitute.clio.transfer.model.ubam.{TransferUbamV1Key, TransferUbamV1Metadata}
import org.broadinstitute.clio.transfer.model.wgscram.{TransferWgsCramV1Key, TransferWgsCramV1Metadata}
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.{EntityId, UpsertId}

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

class ServerService private (
  serverStatusDAO: ServerStatusDAO,
  httpServerDAO: HttpServerDAO,
  persistenceDAO: PersistenceDAO,
  searchDAO: SearchDAO,
  version: String
)(implicit executionContext: ExecutionContext, mat: Materializer)
    extends ModelAutoDerivation
    with StrictLogging {

  /**
    * Kick off a startup, initializing all DAOs
    * and recovering metadata from storage.
    *
    * Returns immediately.
    */
  def beginStartup(): Unit = {
    val startupAttempt = startup()
    startupAttempt.failed.foreach { exception =>
      logger.error(s"Server failed to start due to $exception", exception)
      shutdown()
    }
  }

  /**
    * Recover missing documents for a given Elasticsearch index
    * from the source of truth.
    *
    * Fails immediately if an update pulled from the source of
    * truth fails to be applied to the search index.
    *
    * Returns the number of updates pulled & applied on success.
    */
  private[service] def recoverMetadata(
    implicit index: ElasticsearchIndex[_, _]
  ): Future[Unit] = {
    val name = index.indexName

    searchDAO.getMostRecentDocument.flatMap { mostRecent =>
      val msg = s"Recovering all upserts from index $name"
      val latestUpsert = mostRecent.map(getUpsertId)

      logger.info(latestUpsert.fold(msg)(id => s"$msg since ${id.id}"))

      persistenceDAO
        .getAllSince(latestUpsert)
        .batch(ServerService.RecoveryMaxBulkSize, json => Map(getEntityId(json) -> json)) {
          (idMap, json) =>
            val id = getEntityId(json)
            val newJson = idMap.get(id) match {
              case Some(oldJson) => {
                logger.debug(
                  s"Merging upserts ${getUpsertId(oldJson)} and ${getUpsertId(json)} for id $id"
                )
                oldJson.deepMerge(json)
              }
              case None => json
            }
            idMap.updated(id, newJson)
        }
        .runWith(Sink.foldAsync(()) { (_, jsonMap) =>
          val jsons = jsonMap.valuesIterator.toVector
          searchDAO.updateMetadata(jsons: _*).map { _ =>
            logger.debug(s"Sent ${jsons.size} upserts to Elasticsearch for index $name")
          }
        })
        .map(_ => logger.info(s"Done recovering upserts for index $name"))
    }
  }

  private def getEntityId(json: Json): String =
    json.hcursor
      .get[String](ElasticsearchUtil.toElasticsearchName(EntityId.EntityIdFieldName))
      .fold(throw _, identity)

  private def getUpsertId(json: Json): UpsertId =
    json.hcursor
      .get[UpsertId](ElasticsearchUtil.toElasticsearchName(UpsertId.UpsertIdFieldName))
      .fold(throw _, identity)

  /**
    * Block until shutdown, within some finite limit.
    */
  def awaitShutdown(): Unit = {
    httpServerDAO.awaitShutdown()
  }

  /**
    * Block until shutdown.
    */
  def awaitShutdownInf(): Unit = {
    httpServerDAO.awaitShutdownInf()
  }

  /**
    * Signal a shutdown, but wait until the shutdown occurs.
    */
  def shutdownAndWait(): Unit = {
    shutdown()
    awaitShutdown()
  }

  private[service] def startup(): Future[Unit] = {

    val indexes = immutable.Seq(
      ElasticsearchIndex[TransferUbamV1Key, TransferUbamV1Metadata],
      ElasticsearchIndex[TransferGvcfV1Key, TransferGvcfV1Metadata],
      ElasticsearchIndex[TransferWgsCramV1Key, TransferWgsCramV1Metadata]
    )

    for {
      _ <- serverStatusDAO.setStatus(ClioStatus.Starting)
      _ <- persistenceDAO.initialize(indexes, version)
      _ <- searchDAO.initialize(indexes)
      _ <- httpServerDAO.startup()
      _ <- serverStatusDAO.setStatus(ClioStatus.Recovering)
      _ = logger.info("Recovering metadata from storage...")
      _ <- Future.sequence(indexes.map(recoverMetadata(_)))
      _ <- httpServerDAO.enableApi()
      _ <- serverStatusDAO.setStatus(ClioStatus.Started)
      _ = logger.info("Server started")
    } yield ()
  }

  private[service] def shutdown(): Future[Unit] = {
    serverStatusDAO.setStatus(ClioStatus.ShuttingDown) transformWith { _ =>
      searchDAO.close() transformWith { _ =>
        httpServerDAO.shutdown() transformWith { _ =>
          serverStatusDAO.setStatus(ClioStatus.ShutDown)
        }
      }
    }
  }
}

object ServerService {

  /**
    * Maximum number of updates that should be sent in a single bulk request
    * to Elasticsearch during document recovery.
    */
  val RecoveryMaxBulkSize: Long = 1000L

  def apply(
    app: ClioApp
  )(implicit executionContext: ExecutionContext, mat: Materializer): ServerService = {
    new ServerService(
      app.serverStatusDAO,
      app.httpServerDAO,
      app.persistenceDAO,
      app.searchDAO,
      ClioServerConfig.Version.value
    )
  }
}
