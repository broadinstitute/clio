package org.broadinstitute.clio.integrationtest

import akka.http.scaladsl.model.Uri
import com.dimafeng.testcontainers.TestContainerProxy
import com.typesafe.scalalogging.LazyLogging
import org.testcontainers.containers.DockerComposeContainer

import java.io.File

/**
  * Reimplementation of the important bits from
  * [[com.dimafeng.testcontainers.DockerComposeContainer]],
  * with settings tailored for our integration tests, since
  * testcontainers-scala doesn't make the settings configurable.
  */
class ClioDockerComposeContainer(composeFile: File,
                                 exposedServices: Map[String, Int] = Map.empty)
    extends TestContainerProxy[DockerComposeContainer[_]]
    with LazyLogging {
  import scala.language.existentials

  type OTCContainer = DockerComposeContainer[T] forSome {
    type T <: DockerComposeContainer[T]
  }
  override val container: OTCContainer =
    new DockerComposeContainer(composeFile)
  exposedServices.foreach(Function.tupled(container.withExposedService))

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
