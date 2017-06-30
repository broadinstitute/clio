package org.broadinstitute.clio.server.dataaccess

import com.typesafe.config.ConfigFactory
import org.broadinstitute.clio.server.ClioConfig

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
    private val docker = config.getConfig("docker")
    val elasticsearch: String = docker.getString("elasticsearch")
  }
}
