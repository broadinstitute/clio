package org.broadinstitute.clio

import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._

import scala.concurrent.duration.FiniteDuration

/**
  * Configuration file accessors.
  */
object ClioConfig {
  private val config = {
    // parse dotted names, unlike ConfigFactory.systemEnvironment
    val environment = ConfigFactory.parseMap(System.getenv)
    val default = ConfigFactory.load
    val root = environment.withFallback(default)
    root.getConfig("clio")
  }

  object HttpServer {
    private val http = config.getConfig("http-server")
    val interface = http.getString("interface")
    val port = http.getInt("port")
    val shutdownTimeout = http.as[FiniteDuration]("shutdown-timeout")
  }

  object Version {
    val value = config.getString("version")
  }

  object Elasticsearch {
    case class ElasticsearchHttpHost(hostname: String = "localhost", port: Int = 9200, scheme: String = "http")

    private val elasticsearch = config.getConfig("elasticsearch")
    val httpHosts = elasticsearch.as[Seq[ElasticsearchHttpHost]]("http-hosts")
  }
}
