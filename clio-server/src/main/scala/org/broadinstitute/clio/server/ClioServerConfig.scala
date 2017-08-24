package org.broadinstitute.clio.server

import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.ServiceAccount

import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import net.ceedubs.ficus.readers.ValueReader

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

import java.nio.file.{Files, Path}

/**
  * Configuration file accessors.
  */
object ClioServerConfig {
  private val server = ClioConfig.getConfig("server")

  object Persistence extends ModelAutoDerivation {
    implicit val pathReader: ValueReader[Path] =
      (config: Config, path: String) => ???

    private val persistence = server.as[Config]("persistence")
    private val jsonPath = persistence.getAs[Path]("service-account-json")
    lazy val serviceAccount: Option[ServiceAccount] =
      jsonPath.map { path =>
        import io.circe.parser._
        val jsonBlob =
          Files.readAllLines(path).asScala.mkString("\n").stripMargin
        decode[ServiceAccount](jsonBlob).fold({ error =>
          throw new RuntimeException(
            s"Could not decode service account JSON at $path",
            error
          )
        }, account => account)
      }
  }

  object HttpServer {
    private val http = server.getConfig("http-server")
    val interface: String = http.getString("interface")
    val port: Int = http.getInt("port")
    val shutdownTimeout: FiniteDuration =
      http.as[FiniteDuration]("shutdown-timeout")
  }

  object Version {
    val value: String = server.getString("version")
  }

  object Elasticsearch {
    case class ElasticsearchHttpHost(hostname: String = "localhost",
                                     port: Int = 9200,
                                     scheme: String = "http")

    private val elasticsearch = server.getConfig("elasticsearch")
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
