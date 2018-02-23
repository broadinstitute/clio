package org.broadinstitute.clio.server.dataaccess

import akka.pattern.after
import org.broadinstitute.clio.server.ClioServerConfig
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex

import scala.collection.immutable
import scala.concurrent.Future
import scala.util.{Failure, Success}

trait SystemElasticsearchDAO extends SearchDAO { this: HttpElasticsearchDAO =>

  override def checkOk: Future[Unit] = {
    getClusterHealth transform {
      case Success(health) if health.status == "green" => Success(())
      case Success(health) =>
        val message = s"Health status was not green: $health"
        logger.error(message)
        Failure(new RuntimeException(message))
      case Failure(exception) =>
        logger.error(
          s"Error while getting Elasticsearch cluster status from $hostsString",
          exception
        )
        Failure(exception)
    }
  }

  override def initialize(
    indexes: immutable.Seq[ElasticsearchIndex[_, _]]
  ): Future[Unit] = {
    waitForSearchReady().flatMap { _ =>
      Future.foldLeft(indexes.map(createOrUpdateIndex))(())((_, _) => ())
    }
  }

  override def close(): Future[Unit] = {
    Future {
      closeClient()
    }
  }

  /** If an index with the name doesn't exist then create it, and then separately update the fields on the index. */
  private[dataaccess] def createOrUpdateIndex(
    index: ElasticsearchIndex[_, _]
  ): Future[Unit] = {
    for {
      exists <- existsIndexType(index)
      _ <- if (exists) Future.successful(()) else createIndexType(index)
      _ <- updateFieldDefinitions(index)
    } yield ()
  }

  private def isReady: Future[Boolean] = {
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

  private def hostsString = httpHosts.mkString(", ")

  private[dataaccess] def waitForSearchReady(): Future[Unit] = {
    val waitDuration = ClioServerConfig.Elasticsearch.readinessPatience

    def waitWithRetry(retryCount: Int): Future[Unit] = {
      isReady transformWith {
        case Success(true) => Future.successful(())
        case Success(false) if retryCount <= 0 =>
          for {
            status <- checkOk
          } yield {
            logger.info("Elasticsearch not ready. Giving up.")
            throw new RuntimeException(
              s"Server was not ready for processing. Last status was: $status"
            )
          }
        case Failure(exception) if retryCount <= 0 =>
          logger.error(
            s"Unable to get status due to exception ${exception.getMessage}. Giving up.",
            exception
          )
          Future.failed(exception)
        case Success(false) =>
          logger.info(
            s"Elasticsearch not ready. Will try again in $waitDuration."
          )
          after(waitDuration, system.scheduler)(waitWithRetry(retryCount - 1))
        case Failure(exception) =>
          logger.error(
            s"Unable to get Elasticsearch status due to exception ${exception.getMessage}." +
              s"Will try again in $waitDuration.",
            exception
          )
          after(waitDuration, system.scheduler)(waitWithRetry(retryCount - 1))
      }
    }

    waitWithRetry(ClioServerConfig.Elasticsearch.readinessRetries)
  }
}
