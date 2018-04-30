package org.broadinstitute.clio.server

import better.files.File
import cats.syntax.either._
import com.typesafe.config.{Config, ConfigException}
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import org.broadinstitute.clio.util.auth.ClioCredentials
import org.broadinstitute.clio.util.config.{ClioConfig, ConfigReaders}
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.Location

import scala.concurrent.duration.FiniteDuration

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
    case class LocalConfig(rootDir: Option[File]) extends PersistenceConfig
    case class GcsConfig(projectId: String, bucket: String, creds: ClioCredentials)
        extends PersistenceConfig

    private val persistence: Config =
      serverConfig.as[Config]("persistence")
    private val persistenceType: String = persistence.as[String]("type")

    lazy val config: PersistenceConfig = {
      Location
        .withNameInsensitiveOption(persistenceType)
        .flatMap {
          case Location.OnPrem => {
            val maybeRoot = persistence.getAs[File]("root-dir")
            Some(LocalConfig(maybeRoot))
          }
          case Location.GCP => {
            val projectId = persistence.as[String]("project-id")
            val bucket = persistence.as[String]("bucket")
            val jsonPath = persistence.as[File]("service-account-json")
            val credentials = Either.catchNonFatal(new ClioCredentials(Some(jsonPath)))
            credentials.fold(
              { err =>
                throw new ConfigException.BadValue(
                  "clio.server.persistence.service-account-json",
                  s"Could not load service account JSON from '$jsonPath'",
                  err
                )
              }, { account =>
                Some(GcsConfig(projectId, bucket, account))
              }
            )
          }
        }
        .getOrElse {
          val validValues =
            Location.namesToValuesMap.keys
              .mkString("'", "', '", "'")

          throw new ConfigException.BadValue(
            "clio.server.persistence.type",
            s"Given persistence type '$persistenceType', valid values are $validValues (case insensitive)"
          )
        }
    }

    val recoveryParallelism: Int = persistence.as[Int]("recovery-parallelism")
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
    case class ElasticsearchHttpHost(
      hostname: String = "localhost",
      port: Int = 9200,
      scheme: String = "http"
    )

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

    /**
      * The time to wait for an HTTP Request to complete when communicating with elasticsearch.
      */
    val httpRequestTimeout: FiniteDuration =
      elasticsearch.as[FiniteDuration]("http-request-timeout")
  }
}
