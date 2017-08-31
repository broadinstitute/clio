package org.broadinstitute.clio.integrationtest

import akka.http.scaladsl.model.Uri
import com.dimafeng.testcontainers.DockerComposeContainer

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
   * versions / file paths), but the elasticsearch host to test against varies
   * from suite to suite so we set it here instead.
   */
  container.withEnv("ELASTICSEARCH_HOST", elasticsearchHostname)

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
