package org.broadinstitute.clio.server.dataaccess

import org.broadinstitute.clio.model.ElasticsearchStatusInfo
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex

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
    * Checks if an index exists.
    *
    * @param index The index name and type.
    * @return A future Boolean if the index exists, or a future error.
    */
  def existsIndexType(index: ElasticsearchIndex[_]): Future[Boolean]

  /**
    * Creates an empty index.
    *
    * @param index     The index name and type. Fields are ignored.
    * @return A future Unit after the index is created, or a future error.
    */
  def createIndexType(index: ElasticsearchIndex[_]): Future[Unit]

  /**
    * Updates fields for an existing index.
    *
    * @param index The index name and type to update.
    * @return A future Unit after the fields are updated, or a future error.
    */
  def updateFieldDefinitions(index: ElasticsearchIndex[_]): Future[Unit]

  /**
    * Closes the connection to elasticsearch.
    */
  def close(): Future[Unit]
}
