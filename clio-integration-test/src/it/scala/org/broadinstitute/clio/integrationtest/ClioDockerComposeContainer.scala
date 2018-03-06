package org.broadinstitute.clio.integrationtest

import java.nio.file.StandardOpenOption
import java.time.OffsetDateTime

import akka.http.scaladsl.model.Uri
import better.files.File
import com.dimafeng.testcontainers.DockerComposeContainer
import io.circe.Json
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ElasticsearchIndex,
  ElasticsearchUtil
}
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.UpsertId
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
  seededDocuments: Map[ElasticsearchIndex[_], Seq[Json]] = Map.empty
) extends DockerComposeContainer(
      Seq(composeFile.toJava),
      exposedServices,
      Base58.randomString(6).toLowerCase()
    )
    with ModelAutoDerivation {

  /** Log files to mount into the Elasticsearch containers. */
  private val esLogs = for {
    service <- Seq("elasticsearch1", "elasticsearch2")
    filename <- Seq("docker-cluster.log", "docker-cluster_access.log")
  } yield {
    File(ClioBuildInfo.logDir, service, filename)
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
    val rootPersistenceDir = File(ClioBuildInfo.persistenceDir)

    Seq(File(ClioBuildInfo.logDir), rootPersistenceDir).foreach {
      _.delete(swallowIOExceptions = true).createDirectories()
    }

    if (seededDocuments.nonEmpty) {
      /*
       * Simulate spreading pre-seeded documents over time.
       *
       * NOTE: The spread calculation is meant to work around a problem with case-sensitivity
       * when running this test on OS X. Our `UpsertId`s are case-sensitive, but HFS / APFS are
       * case-insensitive by default. This can cause naming collisions when generating a ton of
       * IDs at once (like in these tests). By spreading documents into bins of 26, we try to
       * avoid the possibility of two IDs differing only in the case of the last byte being dropped
       * into the same day-directory.
       */
      val daySpread = (seededDocuments.map(_._2.size).max / 26).toLong
      val today: OffsetDateTime = OffsetDateTime.now()
      val earliest: OffsetDateTime = today.minusDays(daySpread)

      seededDocuments.foreach {
        case (index, documents) =>
          val documentCount = documents.length

          documents.zipWithIndex.foreach {
            case (json, i) => {
              val dateDir = index.persistenceDirForDatetime(
                earliest.plusDays(i.toLong / (documentCount.toLong / daySpread))
              )

              val writeDir = (rootPersistenceDir / s"$dateDir/").createDirectories()
              val upsertId = json.hcursor
                .get[UpsertId](
                  ElasticsearchUtil.toElasticsearchName(UpsertId.UpsertIdFieldName)
                )
                .fold(throw _, identity)

              val _ = (writeDir / upsertId.persistenceFilename).write(
                defaultPrinter.pretty(json)
              )(Seq(StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE))
            }
          }
      }
    }

    val logPaths = ClioDockerComposeContainer.clioLog +: esLogs
    logPaths.foreach { log =>
      if (!log.parent.exists) {
        val _ = log.parent.createDirectories()
      }
      if (!log.exists) {
        import java.nio.file.attribute.PosixFilePermission._

        val _ = log
          .createFile()
          // Without these permissions, logback throws on startup, breaking our log monitoring.
          .setPermissions(
            Set(
              OWNER_READ,
              OWNER_WRITE,
              GROUP_READ,
              GROUP_WRITE,
              OTHERS_READ,
              OTHERS_WRITE
            )
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
      val target = File(s"$dirPrefix-${description.getDisplayName}")
        .delete(swallowIOExceptions = true)
      val _ = File(dirPrefix).moveTo(target, overwrite = true)
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
  val clioLog: File = File(ClioBuildInfo.logDir, "clio-server", "clio-server.log")
}
