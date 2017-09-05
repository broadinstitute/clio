package org.broadinstitute.clio.client

import org.broadinstitute.clio.util.config.ClioConfig
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

import scala.concurrent.duration.FiniteDuration

/**
  * Configuration file accessors.
  */
object ClioClientConfig {
  private val config = ClioConfig.load

  val responseTimeout: FiniteDuration =
    config.as[FiniteDuration]("client.response-timeout")

  object Version {
    val value: String = config.as[String]("client.version")
  }

  object ClioServer {
    private val clioServer = config.as[Config]("server")
    val clioServerHostName: String = clioServer.as[String]("hostname")
    val clioServerPort: Int = clioServer.as[Int]("port")
    val clioServerUseHttps: Boolean = clioServer.as[Boolean]("use-https")
  }

  val greenTeamEmail: String = config.as[String]("greenteam.email")

  val serviceAccountJson: Option[String] =
    config.as[Option[String]]("service-account-json")
}
