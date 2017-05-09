package org.broadinstitute.clio.dataaccess

import com.dimafeng.testcontainers.{ForAllTestContainer, GenericContainer}
import org.scalatest.Suite
import org.testcontainers.containers.wait.Wait

/**
  * Elasticsearch Test Container
  */
trait ElasticsearchContainer extends ForAllTestContainer {
  self: Suite =>

  // Adapted from https://www.elastic.co/guide/en/elasticsearch/reference/current/docker.html
  val container =
    GenericContainer(
      imageName = TestContainers.DockerImages.elasticsearch,
      exposedPorts = Seq(9200),
      env = Map(
        "ES_JAVA_OPTS" -> "-Xms512m -Xmx512m",
        "xpack.security.enabled" -> "false"
      ),
      waitStrategy = Wait.forHttp("/")
    )

  lazy val elasticsearchContainerIpAddress = container.container.getContainerIpAddress

  lazy val elasticsearchPort = container.container.getMappedPort(9200)
}
