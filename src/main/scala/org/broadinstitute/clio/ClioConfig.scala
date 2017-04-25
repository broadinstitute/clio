package org.broadinstitute.clio

import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._

import scala.concurrent.duration.FiniteDuration

/**
  * Configuration file accessors.
  */
object ClioConfig {
  private val config = ConfigFactory.load.getConfig("clio")

  object HttpServer {
    private val http = config.getConfig("http-server")
    val interface = http.getString("interface")
    val port = http.getInt("port")
    val shutdownTimeout = http.as[FiniteDuration]("shutdown-timeout")
  }

  object Version {
    val value = config.getString("version")
  }
}
