package org.broadinstitute.clio.server.service

import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.typesafe.scalalogging.StrictLogging
import io.circe.Decoder
import org.broadinstitute.clio.server.ClioApp
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ClioDocument,
  ElasticsearchIndex
}
import org.broadinstitute.clio.server.dataaccess.{
  HttpServerDAO,
  PersistenceDAO,
  SearchDAO,
  ServerStatusDAO
}
import org.broadinstitute.clio.status.model.ServerStatusInfo
import org.broadinstitute.clio.util.json.ModelAutoDerivation

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

class ServerService private (
  serverStatusDAO: ServerStatusDAO,
  httpServerDAO: HttpServerDAO,
  persistenceDAO: PersistenceDAO,
  searchDAO: SearchDAO
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
  private[service] def recoverMetadata[D <: ClioDocument: Decoder](
    index: ElasticsearchIndex[D]
  ): Future[Int] = {
    searchDAO.getMostRecentDocument(index).flatMap { mostRecent =>
      persistenceDAO
        .getAllSince(mostRecent, index)
        .runWith(Sink.foldAsync(0) { (count, doc) =>
          logger.debug(s"Recovering document with ID ${doc.upsertId}")
          searchDAO.updateMetadata(doc, index).map(_ => count + 1)
        })
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
      ElasticsearchIndex.WgsCram
    )

    for {
      _ <- serverStatusDAO.setStatus(ServerStatusInfo.Starting)
      _ <- persistenceDAO.initialize(indexes)
      _ <- searchDAO.initialize(indexes)
      _ = logger.info("Recovering metadata from storage...")
      Seq(recoveredUbamCount, recoveredGvcfCount, recoveredCramCount) <- Future
        .sequence(
          Seq(
            recoverMetadata(ElasticsearchIndex.WgsUbam),
            recoverMetadata(ElasticsearchIndex.Gvcf),
            recoverMetadata(ElasticsearchIndex.WgsCram)
          )
        )
      _ = logger.info(s"Recovered $recoveredUbamCount wgs-ubams from storage")
      _ = logger.info(s"Recovered $recoveredGvcfCount gvcfs from storage")
      _ = logger.info(s"Recovered $recoveredCramCount wgs-crams from storage")
      _ <- httpServerDAO.startup()
      _ <- serverStatusDAO.setStatus(ServerStatusInfo.Started)
      _ = logger.info("Server started")
    } yield ()
  }

  private[service] def shutdown(): Future[Unit] = {
    serverStatusDAO.setStatus(ServerStatusInfo.ShuttingDown) transformWith {
      _ =>
        searchDAO.close() transformWith { _ =>
          httpServerDAO.shutdown() transformWith { _ =>
            serverStatusDAO.setStatus(ServerStatusInfo.ShutDown)
          }
        }
    }
  }
}

object ServerService {
  def apply(app: ClioApp)(implicit executionContext: ExecutionContext,
                          mat: Materializer): ServerService = {
    new ServerService(
      app.serverStatusDAO,
      app.httpServerDAO,
      app.persistenceDAO,
      app.searchDAO
    )
  }
}
