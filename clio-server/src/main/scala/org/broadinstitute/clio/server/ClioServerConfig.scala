package org.broadinstitute.clio.server

import org.broadinstitute.clio.util.config.{ClioConfig, ConfigReaders}
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.ServiceAccount

import com.typesafe.config.{Config, ConfigException}
import enumeratum.{Enum, EnumEntry}
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._

import scala.collection.immutable.IndexedSeq
import scala.concurrent.duration.FiniteDuration
import scala.io.Source

import java.nio.file.Path

/**
  * Configuration file accessors.
  */
object ClioServerConfig extends ConfigReaders {
  private val serverConfig = ClioConfig.load.as[Config]("server")

  object Persistence extends ModelAutoDerivation {

    /**
      * Types of persistence supported by Clio.
      */
    sealed trait Type extends EnumEntry
    object Type extends Enum[Type] {
      override val values: IndexedSeq[Type] = findValues
      case object Local extends Type
      case object Gcs extends Type
    }

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
      Type.lowerCaseNamesToValuesMap
        .get(persistenceType)
        .map {
          case Type.Local => {
            val maybeRoot = persistence.getAs[Path]("root-dir")
            LocalConfig(maybeRoot)
          }
          case Type.Gcs => {
            val projectId = persistence.as[String]("project-id")
            val bucket = persistence.as[String]("bucket")
            val jsonPath = persistence.as[Path]("service-account-json")
            val serviceAccount = {
              import io.circe.parser._
              val jsonBlob =
                Source.fromFile(jsonPath.toFile).mkString.stripMargin
              decode[ServiceAccount](jsonBlob).fold({ error =>
                throw new RuntimeException(
                  s"Could not decode service account JSON at $jsonPath",
                  error
                )
              }, identity)
            }
            GcsConfig(projectId, bucket, serviceAccount)
          }
        }
        .getOrElse {
          val validValues =
            Type.lowerCaseNamesToValuesMap.keys.mkString("'", "', '", "'")

          throw new ConfigException.BadValue(
            "clio.server.persistence.type",
            s"Given persistence type '$persistenceType', valid values are $validValues"
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
