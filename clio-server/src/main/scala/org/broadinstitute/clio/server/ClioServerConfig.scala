package org.broadinstitute.clio.server

import org.broadinstitute.clio.util.model.Env

import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import net.ceedubs.ficus.readers.ValueReader

import scala.concurrent.duration.FiniteDuration

/**
  * Configuration file accessors.
  */
object ClioServerConfig {
  private val config = ClioConfig.getConfig("server")

  object Environment {
    implicit val envReader = new ValueReader[Env] {
      override def read(config: Config, path: String): Env = {
        val maybeEnv = config.as[String](path)
        Env.withName(maybeEnv)
      }
    }
    val value: Env = config.as[Env]("environment")
  }

  object HttpServer {
    private val http = config.getConfig("http-server")
    val interface: String = http.getString("interface")
    val port: Int = http.getInt("port")
    val shutdownTimeout: FiniteDuration =
      http.as[FiniteDuration]("shutdown-timeout")
  }

  object Version {
    val value: String = config.getString("version")
  }

  object Elasticsearch {
    case class ElasticsearchHttpHost(hostname: String = "localhost",
                                     port: Int = 9200,
                                     scheme: String = "http")

    private val elasticsearch = config.getConfig("elasticsearch")
    val replicateIndices: Boolean =
      elasticsearch.getBoolean("replicate-indices")
    val httpHosts: Seq[ElasticsearchHttpHost] =
      elasticsearch.as[Seq[ElasticsearchHttpHost]]("http-hosts")
    val readinessColors: Seq[String] =
      elasticsearch.as[Seq[String]]("readiness.colors")
    val readinessRetries: Int = elasticsearch.getInt("readiness.retries")
    val readinessPatience: FiniteDuration =
      elasticsearch.as[FiniteDuration]("readiness.patience")
  }
}
