package org.broadinstitute.clio.server.dataaccess

import scala.concurrent.Future

/**
  * Starts and stops an http server.
  */
trait HttpServerDAO {

  /**
    * Starts the web server, exposing health and version
    * endpoints but not the upsert or query APIs.
    */
  def startup(): Future[Unit]

  /**
    * Exposes the upsert and query endpoints.
    */
  def enableApi(): Future[Unit]

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
