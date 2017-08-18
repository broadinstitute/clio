package org.broadinstitute.clio.client
import com.typesafe.config.{Config, ConfigFactory}
import net.ceedubs.ficus.Ficus._

import scala.concurrent.duration.FiniteDuration

/**
  * Configuration file accessors.
  */
object ClioClientConfig {
  private val environment = ConfigFactory.parseMap(System.getenv)

  /**
    * Enable the passed in config to be overridden via environment variables.
    *
    * NOTE: Mimic of this behavior exists within the ClioIntegrationTestRunner also.
    */
  def withEnvironment(config: Config): Config = {
    environment.withFallback(config).getConfig("clio")
  }

  private val config: Config = withEnvironment(ConfigFactory.load)

  val responseTimeout: FiniteDuration =
    config.as[FiniteDuration]("client.response-timeout")

  /** Retrieve a clio configuration at a path. */
  def getConfig(path: String): Config = config.getConfig(path)

  object Version {
    val value: String = config.as[String]("client.version")
  }

  object ClioServer {
    private val clioServer = config.getConfig("server")
    val clioServerHostName: String = clioServer.as[String]("hostname")
    val clioServerPort: Int = clioServer.as[Int]("port")
    val clioServerUseHttps: Boolean = clioServer.as[Boolean]("use-https")
  }
}
