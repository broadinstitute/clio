package org.broadinstitute.clio.server

object ClioMain {
  def main(args: Array[String]): Unit = {
    sys addShutdownHook ClioServer.shutdownAndWait()
    ClioServer.beginStartup()
    ClioServer.awaitShutdownInf()
  }
}
