package org.broadinstitute.clio.integrationtest

import akka.http.scaladsl.model.Uri
import com.dimafeng.testcontainers.DockerComposeContainer

import scala.collection.JavaConverters._

import java.io.File

/**
  * Extension of [[com.dimafeng.testcontainers.DockerComposeContainer]],
  * with settings tailored for our integration tests, since
  * testcontainers-scala doesn't make the settings configurable.
  */
class ClioDockerComposeContainer(composeFile: File,
                                 elasticsearchHostname: String,
                                 exposedServices: Map[String, Int])
    extends DockerComposeContainer(composeFile, exposedServices) {
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
      ClioDockerComposeContainer.clioLogFileVariable -> ClioBuildInfo.clioLog,
      ClioDockerComposeContainer.persistenceDirVariable -> ClioBuildInfo.persistenceDir
    ).asJava
  )

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
}
