package org.broadinstitute.clio.server

import com.typesafe.scalalogging.LazyLogging

object ClioMain extends LazyLogging {
  def main(args: Array[String]): Unit = {
    sys addShutdownHook ClioServer.shutdownAndWait()
    ClioServer.run()
  }
}
