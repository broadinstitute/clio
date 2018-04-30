package org.broadinstitute.clio.client

import better.files.File
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.clio.util.config.{ClioConfig, ConfigReaders}

import scala.concurrent.duration.FiniteDuration

/**
  * Configuration file accessors.
  */
object ClioClientConfig extends ConfigReaders {
  private val config = ClioConfig.load

  private val clientConfig = config.as[Config]("client")

  val responseTimeout: FiniteDuration =
    clientConfig.as[FiniteDuration]("response-timeout")

  val maxRequestRetries: Int =
    clientConfig.as[Int]("max-request-retries")

  val serviceAccountJson: Option[File] =
    clientConfig.getAs[File]("service-account-json")

  object Version {
    val value: String = config.as[String]("client.version")
  }

  object ClioServer {
    private val clioServer = config.as[Config]("server")
    val clioServerHostName: String = clioServer.as[String]("hostname")
    val clioServerPort: Int = clioServer.as[Int]("port")
    val clioServerUseHttps: Boolean = clioServer.as[Boolean]("use-https")
  }
}
