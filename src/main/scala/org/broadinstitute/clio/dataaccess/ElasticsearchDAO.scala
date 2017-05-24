package org.broadinstitute.clio.dataaccess

import org.broadinstitute.clio.model.ElasticsearchStatusInfo

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

/**
  * Communicates with an elasticsearch server.
  */
trait ElasticsearchDAO {
  /**
    * Returns the status.
    */
  def getClusterStatus: Future[ElasticsearchStatusInfo]

  /**
    * Returns true if elasticsearch is ready for use.
    */
  def isReady: Future[Boolean]

  /**
    * Number of times to check if elasticsearch is ready.
    */
  val readyRetries: Int

  /**
    * How much time to wait between checks of readiness.
    */
  val readyPatience: FiniteDuration

  /**
    * Closes the connection to elasticsearch.
    */
  def close(): Future[Unit]
}
