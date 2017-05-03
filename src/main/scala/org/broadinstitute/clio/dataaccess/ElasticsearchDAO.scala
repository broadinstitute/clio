package org.broadinstitute.clio.dataaccess

import org.broadinstitute.clio.model.ElasticsearchStatusInfo

import scala.concurrent.Future

/**
  * Communicates with an elasticsearch server.
  */
trait ElasticsearchDAO {
  /**
    * Returns the status.
    */
  def getClusterStatus: Future[ElasticsearchStatusInfo]

  /**
    * Closes the connection to elasticsearch.
    */
  def close(): Future[Unit]
}
