package org.broadinstitute.clio.server

import org.broadinstitute.clio.util.config.{ClioConfig, ConfigReaders}
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.ServiceAccount

import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._

import scala.concurrent.duration.FiniteDuration
import scala.io.Source

import java.nio.file.Path

/**
  * Configuration file accessors.
  */
object ClioServerConfig extends ConfigReaders {
  private val serverConfig = ClioConfig.load.as[Config]("server")

  object Persistence extends ModelAutoDerivation {
    private val persistence = serverConfig.as[Config]("persistence")
    private val jsonPath = persistence.getAs[Path]("service-account-json")
    lazy val serviceAccount: Option[ServiceAccount] =
      jsonPath.map { path =>
        import io.circe.parser._
        val jsonBlob =
          Source.fromFile(path.toFile).mkString.stripMargin
        decode[ServiceAccount](jsonBlob).fold({ error =>
          throw new RuntimeException(
            s"Could not decode service account JSON at $path",
            error
          )
        }, identity)
      }
  }

  object HttpServer {
    private val http = serverConfig.as[Config]("http-server")
    val interface: String = http.as[String]("interface")
    val port: Int = http.as[Int]("port")
    val shutdownTimeout: FiniteDuration =
      http.as[FiniteDuration]("shutdown-timeout")
  }

  object Version {
    val value: String = serverConfig.as[String]("version")
  }

  object Elasticsearch {
    case class ElasticsearchHttpHost(hostname: String = "localhost",
                                     port: Int = 9200,
                                     scheme: String = "http")

    private val elasticsearch = serverConfig.as[Config]("elasticsearch")
    val replicateIndices: Boolean =
      elasticsearch.as[Boolean]("replicate-indices")
    val httpHosts: Seq[ElasticsearchHttpHost] =
      elasticsearch.as[Seq[ElasticsearchHttpHost]]("http-hosts")
    val readinessColors: Seq[String] =
      elasticsearch.as[Seq[String]]("readiness.colors")
    val readinessRetries: Int = elasticsearch.as[Int]("readiness.retries")
    val readinessPatience: FiniteDuration =
      elasticsearch.as[FiniteDuration]("readiness.patience")
  }
}
