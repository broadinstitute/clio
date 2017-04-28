package org.broadinstitute.clio.dataaccess

import com.dimafeng.testcontainers.{ForAllTestContainer, GenericContainer}
import org.scalatest.Suite
import org.testcontainers.containers.wait.Wait

/**
  * Elasticsearch Test Container
  */
trait ElasticsearchContainer extends ForAllTestContainer {
  self: Suite =>

  // docker-java can't read configs that reference OSX keystores, etc.
  // https://github.com/docker-java/docker-java/issues/806
  if (!sys.props.keys.exists(_ == "DOCKER_CONFIG")) {
    sys.props += "DOCKER_CONFIG" -> "/dev/null/workaround/docker-java/issues/806"
  }

  // Adapted from https://www.elastic.co/guide/en/elasticsearch/reference/current/docker.html
  override val container: GenericContainer = GenericContainer(
    imageName = s"docker.elastic.co/elasticsearch/elasticsearch:${ElasticsearchContainer.ElasticsearchVersion}",
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

object ElasticsearchContainer {
  val ElasticsearchVersion = "5.3.0"
}
