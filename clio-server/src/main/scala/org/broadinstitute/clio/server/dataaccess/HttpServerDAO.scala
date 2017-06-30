package org.broadinstitute.clio.dataaccess

import scala.concurrent.Future

/**
  * Starts and stops an http server.
  */
trait HttpServerDAO {

  /**
    * Starts the web server.
    */
  def startup(): Future[Unit]

  /**
    * Returns the server version.
    */
  def getVersion: Future[String]

  /**
    * Shuts down the server.
    */
  def shutdown(): Future[Unit]

  /**
    * Waits a short duration for the system to shutdown.
    */
  def awaitShutdown(): Unit

  /**
    * Waits forever for the system to shutdown.
    */
  def awaitShutdownInf(): Unit
}
