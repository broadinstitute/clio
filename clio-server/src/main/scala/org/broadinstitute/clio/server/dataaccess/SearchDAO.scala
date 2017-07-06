package org.broadinstitute.clio.server.dataaccess

import scala.concurrent.Future

/**
  * Communicates with a search server.
  */
trait SearchDAO {

  /**
    * Checks the status.
    */
  def checkOk: Future[Unit]

  /**
    * Initialize the ready search application.
    */
  def initialize(): Future[Unit]

  /**
    * Closes the connection.
    */
  def close(): Future[Unit]
}
