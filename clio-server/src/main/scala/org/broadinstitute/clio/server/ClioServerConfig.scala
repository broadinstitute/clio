package org.broadinstitute.clio.server

import org.broadinstitute.clio.util.config.{ClioConfig, ConfigReaders}
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.{Location, ServiceAccount}
import com.typesafe.config.{Config, ConfigException}
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import scala.concurrent.duration.FiniteDuration
import java.nio.file.Path

import org.broadinstitute.clio.util.AuthUtil

/**
  * Configuration file accessors.
  */
object ClioServerConfig extends ConfigReaders {
  private val serverConfig = ClioConfig.load.as[Config]("server")

  object Persistence extends ModelAutoDerivation {

    /**
      * Configuration containers for each of the
      * persistence types supported by Clio.
      */
    sealed trait PersistenceConfig
    case class LocalConfig(rootDir: Option[Path]) extends PersistenceConfig
    case class GcsConfig(projectId: String,
                         bucket: String,
                         account: ServiceAccount)
        extends PersistenceConfig

    private val persistence: Config =
      serverConfig.as[Config]("persistence")
    private val persistenceType: String = persistence.as[String]("type")

    lazy val config: PersistenceConfig = {
      Location
        .withNameInsensitiveOption(persistenceType)
        .flatMap {
          case Location.OnPrem => {
            val maybeRoot = persistence.getAs[Path]("root-dir")
            Some(LocalConfig(maybeRoot))
          }
          case Location.GCP => {
            val projectId = persistence.as[String]("project-id")
            val bucket = persistence.as[String]("bucket")
            val jsonPath = persistence.as[Path]("service-account-json")
            val serviceAccount = AuthUtil.loadServiceAccountJson(jsonPath)
            Some(GcsConfig(projectId, bucket, serviceAccount))
          }
          case Location.Unknown => None
        }
        .getOrElse {
          val validValues =
            Location.namesToValuesMap
              .filterNot(_._2 == Location.Unknown)
              .keys
              .mkString("'", "', '", "'")

          throw new ConfigException.BadValue(
            "clio.server.persistence.type",
            s"Given persistence type '$persistenceType', valid values are $validValues (case insensitive)"
          )
        }
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
