package org.broadinstitute.clio.server.service

import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.typesafe.scalalogging.StrictLogging
import org.broadinstitute.clio.server.dataaccess.elasticsearch._
import org.broadinstitute.clio.server.dataaccess.{
  HttpServerDAO,
  PersistenceDAO,
  SearchDAO,
  ServerStatusDAO
}
import org.broadinstitute.clio.status.model.ClioStatus
import org.broadinstitute.clio.util.json.ModelAutoDerivation

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

class ServerService private[server] (
  serverStatusDAO: ServerStatusDAO,
  persistenceDAO: PersistenceDAO,
  searchDAO: SearchDAO,
  httpServerDAO: HttpServerDAO,
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
    implicit index: ElasticsearchIndex[_]
  ): Future[Unit] = {
    val name = index.indexName

    searchDAO.getMostRecentDocument(index).flatMap { mostRecent =>
      val msg = s"Recovering all upserts from index $name"
      val latestUpsert = mostRecent.map(ElasticsearchIndex.getUpsertId)

      logger.info(latestUpsert.fold(msg)(id => s"$msg since ${id.id}"))

      persistenceDAO
        .getAllSince(latestUpsert)
        .batch(
          ServerService.RecoveryMaxBulkSize,
          json => Map(ElasticsearchIndex.getEntityId(json) -> json)
        ) { (idMap, json) =>
          val id = ElasticsearchIndex.getEntityId(json)
          val newJson = idMap.get(id) match {
            case Some(oldJson) => {
              val oldId = ElasticsearchIndex.getUpsertId(oldJson)
              val newId = ElasticsearchIndex.getUpsertId(json)
              logger.debug(
                s"Merging upserts $oldId and $newId for id $id"
              )
              oldJson.deepMerge(json)
            }
            case None => json
          }
          idMap.updated(id, newJson)
        }
        .runWith(Sink.foldAsync(()) { (_, jsonMap) =>
          val jsons = jsonMap.values.toSeq
          searchDAO.updateMetadata(jsons).map { _ =>
            logger.debug(s"Sent ${jsons.size} upserts to Elasticsearch for index $name")
          }
        })
        .map(_ => logger.info(s"Done recovering upserts for index $name"))
    }
  }

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
      ElasticsearchIndex.WgsUbam,
      ElasticsearchIndex.Gvcf,
      ElasticsearchIndex.WgsCram,
      ElasticsearchIndex.Arrays
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
}
