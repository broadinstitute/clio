package org.broadinstitute.clio.service

import org.broadinstitute.clio.ClioApp
import org.broadinstitute.clio.dataaccess.HttpServerDAO

class ServerService private(httpServerDAO: HttpServerDAO) {
  /**
    * Kick off a startup, and return immediately.
    */
  def beginStartup(): Unit = {
    httpServerDAO.startup()
    ()
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
    httpServerDAO.shutdown()
    httpServerDAO.awaitShutdown()
  }
}

object ServerService {
  def apply(app: ClioApp): ServerService = {
    new ServerService(app.httpServerDAO)
  }
}
