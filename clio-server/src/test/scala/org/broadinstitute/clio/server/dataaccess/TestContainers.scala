package org.broadinstitute.clio.server.dataaccess

import org.broadinstitute.clio.util.config.ClioConfig

import java.util.concurrent.TimeUnit
import com.typesafe.config.{Config, ConfigFactory}
import net.ceedubs.ficus.Ficus._
import org.testcontainers.images.RemoteDockerImage

import scala.concurrent.duration._

object TestContainers {
  // docker-java can't read configs that reference OSX keystores, etc.
  // https://github.com/docker-java/docker-java/issues/806
  if (!sys.props.keys.exists(_ == "DOCKER_CONFIG")) {
    sys.props += "DOCKER_CONFIG" -> "/dev/null/workaround/docker-java/issues/806"
  }

  private val config = ClioConfig.withEnvironment(
    ConfigFactory.parseResources("clio-docker-images.conf")
  )

  object DockerImages {
    private val docker = config.as[Config]("clio.docker")
    val elasticsearch: String = findRealImageName(
      docker.as[String]("elasticsearch")
    )
  }

  private val DockerIOPrefix = "docker.io/"

  /**
    * Attempts to resolve and pull the docker image name compatible with Testcontainers.
    *
    * On some systems, "docker.io/" gets automagically prepended to the image names, causing failures during
    * `new RemoteDockerImage(image).get()`.
    *
    * https://github.com/testcontainers/testcontainers-java/issues/384
    *
    * @param image   Name of the image.
    * @param timeout Amount of time to wait.
    * @return The image name recognized by Testcontainers.
    */
  private def findRealImageName(
      image: String,
      timeout: FiniteDuration = 60.seconds
  ): String = {
    try {
      new RemoteDockerImage(image).get(timeout.toSeconds, TimeUnit.SECONDS)
      image
    } catch {
      case originalException: Exception if !image.startsWith(DockerIOPrefix) =>
        try {
          findRealImageName(s"$DockerIOPrefix$image", timeout)
        } catch {
          case _: Exception => throw originalException
        }
    }
  }
}
