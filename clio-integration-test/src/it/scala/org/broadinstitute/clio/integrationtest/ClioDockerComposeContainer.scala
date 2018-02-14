package org.broadinstitute.clio.integrationtest

import java.io.File
import java.nio.file.attribute.PosixFilePermissions
import java.nio.file.{Files, Path, Paths}
import java.time.OffsetDateTime

import akka.http.scaladsl.model.Uri
import com.dimafeng.testcontainers.DockerComposeContainer
import io.circe.Encoder
import io.circe.syntax._
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ClioDocument,
  ElasticsearchIndex
}
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.junit.runner.Description
import org.testcontainers.utility.Base58

import scala.collection.JavaConverters._

/**
  * Extension of [[com.dimafeng.testcontainers.DockerComposeContainer]],
  * with settings tailored for our integration tests, since
  * testcontainers-scala doesn't make the settings configurable.
  */
class ClioDockerComposeContainer(
  composeFile: File,
  elasticsearchHostname: String,
  exposedServices: Map[String, Int],
  seededDocuments: Map[ElasticsearchIndex[_], Seq[ClioDocument]] = Map.empty
) extends DockerComposeContainer(
      Seq(composeFile),
      exposedServices,
      Base58.randomString(6).toLowerCase()
    ) {

  /** Log files to mount into the Elasticsearch containers. */
  private val esLogs = for {
    service <- Seq("elasticsearch1", "elasticsearch2")
    filename <- Seq("docker-cluster.log", "docker-cluster_access.log")
  } yield {
    Paths.get(ClioBuildInfo.logDir, service, filename)
  }

  /*
   * Skip "docker pull" because it'll fail if we're testing against a non-
   * pushed version of clio-server.
   */
  container.withPull(false)

  /*
   * Testcontainers doesn't pass env vars onto the docker-compose container it
   * spins up, and we need the vars in order to fill in our compose config.
   * Using the local install gets the vars to pass through.
   */
  container.withLocalCompose(true)

  /*
   * Most of the environment variables needed to fill in our compose files are
   * constants across test suites and passed in by SBT (because they deal with
   * versions / file paths), but the elasticsearch host to test against might vary
   * from suite to suite so it comes in as a constructor parameter instead.
   */
  container.withEnv(
    Map(
      ClioDockerComposeContainer.clioVersionVariable -> ClioBuildInfo.version,
      ClioDockerComposeContainer.elasticsearchVersionVariable -> ClioBuildInfo.elasticsearchVersion,
      ClioDockerComposeContainer.elasticsearchHostVariable -> elasticsearchHostname,
      ClioDockerComposeContainer.configDirVariable -> ClioBuildInfo.confDir,
      ClioDockerComposeContainer.logDirVariable -> ClioBuildInfo.logDir,
      ClioDockerComposeContainer.clioLogFileVariable -> ClioDockerComposeContainer.clioLog.toString,
      ClioDockerComposeContainer.persistenceDirVariable -> ClioBuildInfo.persistenceDir
    ).asJava
  )

  /**
    * Make sure log files exist with global rw permissions before starting containers.
    *
    * Global rw permissions are needed so the integration tests can run on Jenkins,
    * which uses an older version of Docker that bind-mounts files into containers
    * with weird uids and gids. If permissions aren't set properly, log4j will fail
    * to initialize in the ES containers and tests will fail to start.
    */
  override def starting()(implicit description: Description): Unit = {
    val rootPersistenceDir = Paths.get(ClioBuildInfo.persistenceDir)

    Seq(Paths.get(ClioBuildInfo.logDir), rootPersistenceDir).foreach { dir =>
      if (Files.exists(dir)) {
        IoUtil.deleteDirectoryRecursively(dir)
      }
      val _ = Files.createDirectories(dir)
    }

    // Simulate spreading pre-seeded documents over time.
    val daySpread = 10L
    val today: OffsetDateTime = OffsetDateTime.now()
    val earliest: OffsetDateTime = today.minusDays(daySpread)

    seededDocuments.foreach {
      case (index, documents) =>
        val documentCount = documents.length
        documents.zipWithIndex.foreach {
          case (doc, i) => {
            val dateDir = index.persistenceDirForDatetime(
              earliest.plusDays(i.toLong / (documentCount.toLong / daySpread))
            )

            val writeDir =
              Files.createDirectories(rootPersistenceDir.resolve(s"$dateDir/"))

            val json = doc.asJson(index.encoder.asInstanceOf[Encoder[ClioDocument]])

            val _ = Files.write(
              writeDir.resolve(ClioDocument.persistenceFilename(doc.upsertId)),
              ModelAutoDerivation.defaultPrinter.pretty(json).getBytes
            )
          }
        }
    }

    val logPaths = ClioDockerComposeContainer.clioLog +: esLogs
    logPaths.foreach { log =>
      if (!Files.exists(log.getParent)) {
        val _ = Files.createDirectories(log.getParent)
      }
      if (!Files.exists(log)) {
        val _ = Files.createFile(log)
        Files.setPosixFilePermissions(
          log,
          PosixFilePermissions.fromString("rw-rw-rw-")
        )
      }
    }
    super.starting()
  }

  /**
    * After the container stops, move its logs and storage so
    * they don't interfere with subsequent test suites.
    */
  override def finished()(implicit description: Description): Unit = {
    super.finished()
    Seq(ClioBuildInfo.logDir, ClioBuildInfo.persistenceDir).foreach { dirPrefix =>
      val target = Paths.get(s"$dirPrefix-${description.getDisplayName}")
      if (Files.exists(target)) {
        IoUtil.deleteDirectoryRecursively(target)
      }
      val _ = Files.move(Paths.get(dirPrefix), target)
    }
  }

  /**
    * Get the exposed hostname for one of the exposed services running in the underlying container.
    *
    * Will throw an exception if called on a name that wasn't given as an exposed
    * service at construction.
    */
  def getServiceHost(name: String): String =
    container.getServiceHost(name, exposedServices(name))

  /**
    * Get the exposed port for one of the exposed services running in the underlying container.
    *
    * Will throw an exception if called on a name that wasn't given as an exposed
    * service at construction.
    */
  def getServicePort(name: String): Int =
    container.getServicePort(name, exposedServices(name))

  /**
    * Get the URI of one of the exposed services running in the underlying container.
    *
    * Will throw an exception if called on a name that wasn't given as an exposed
    * service at construction.
    */
  def getServiceUri(name: String): Uri = Uri(
    s"http://${getServiceHost(name)}:${getServicePort(name)}"
  )
}

/**
  * Names of environment variables expected to be filled in by our docker-compose files.
  * The values for these fields are mostly provided by SBT using sbt-buildinfo.
  */
object ClioDockerComposeContainer {

  /** Variable used to set the version of clio-server run during tests. */
  val clioVersionVariable = "CLIO_DOCKER_TAG"

  /** Variable used to set the version of elasticsearch run during tests. */
  val elasticsearchVersionVariable = "ELASTICSEARCH_DOCKER_TAG"

  /**
    * Variable used to tell clio-server the hostname of the elasticsearch node
    * to which it should connect.
    */
  val elasticsearchHostVariable = "ELASTICSEARCH_HOST"

  /** Variable used to mount the configuration directory into our containers. */
  val configDirVariable = "CONF_DIR"

  /** Variable used to mount log directories into our containers. */
  val logDirVariable = "LOG_DIR"

  /** Variable used to mount the clio-server log file into its container. */
  val clioLogFileVariable = "CLIO_LOG_FILE"

  /**
    * Variable used to mount a local directory into the clio-server container,
    * for use as a "source of truth" when persisting metadata updates.
    */
  val persistenceDirVariable = "LOCAL_PERSISTENCE_DIR"

  /** Log file to mount into the Clio container. */
  val clioLog: Path =
    Paths.get(ClioBuildInfo.logDir, "clio-server", "clio-server.log")
}
