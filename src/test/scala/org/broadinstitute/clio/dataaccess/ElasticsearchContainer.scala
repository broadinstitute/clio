package org.broadinstitute.clio.dataaccess

import java.util.concurrent.TimeUnit

import com.dimafeng.testcontainers.{ForAllTestContainer, GenericContainer}
import org.scalatest.Suite
import org.testcontainers.containers.wait.Wait
import org.testcontainers.images.RemoteDockerImage

import scala.concurrent.duration._

/**
  * Elasticsearch Test Container
  *
  * Setup adapted from [[https://www.elastic.co/guide/en/elasticsearch/reference/current/docker.html]].
  *
  * X-Pack disabled so that logins aren't required.
  *
  * Transport and HTTP hosts explicitly wired to local IP addresses so that Elasticsearch's automatic bootstrap checks
  * are never initiated.
  * - [[https://www.elastic.co/guide/en/elasticsearch/reference/5.1/docker.html#docker-cli-run-dev-mode]]
  * - [[https://www.elastic.co/blog/bootstrap_checks_annoying_instead_of_devastating]]
  */
trait ElasticsearchContainer extends ForAllTestContainer { self: Suite =>

  override val container: GenericContainer = {
    GenericContainer(
      imageName = ElasticsearchContainer.findRealImageName(
        TestContainers.DockerImages.elasticsearch
      ),
      exposedPorts = Seq(9200),
      env = Map(
        "transport.host" -> "127.0.0.1",
        "http.host" -> "0.0.0.0",
        "ES_JAVA_OPTS" -> "-Xms512m -Xmx512m",
        "xpack.security.enabled" -> "false"
      ),
      waitStrategy = Wait.forHttp("/")
    )
  }

  lazy val elasticsearchContainerIpAddress: String =
    container.container.getContainerIpAddress

  lazy val elasticsearchPort: Integer = container.container.getMappedPort(9200)
}

object ElasticsearchContainer {
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
