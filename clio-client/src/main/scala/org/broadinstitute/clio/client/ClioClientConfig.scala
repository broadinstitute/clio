package org.broadinstitute.clio.client

import java.time.{Duration, Instant}
import java.util.Date

import better.files.File
import com.google.auth.oauth2.AccessToken
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

  val maxQueuedRequests: Int =
    clientConfig.as[Int]("max-queued-requests")

  val maxConcurrentRequests: Int =
    clientConfig.as[Int]("max-concurrent-requests")

  val responseTimeout: FiniteDuration =
    clientConfig.as[FiniteDuration]("response-timeout")

  val maxRequestRetries: Int =
    clientConfig.as[Int]("max-request-retries")

  val greenTeamEmail: String = clientConfig.as[String]("greenteam.email")

  val serviceAccountJson: Option[File] =
    clientConfig.getAs[File]("service-account-json")

  val accessToken: Option[AccessToken] =
    clientConfig
      .getAs[String]("access-token")
      .map(
        t =>
          new AccessToken(
            t,
            Date.from(Instant.now().plus(Duration.ofMinutes(20)))
        )
      )

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
