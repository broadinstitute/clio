package org.broadinstitute.clio.integrationtest

import sbt._

/** Custom SBT keys for our integration-test process. */
trait ClioIntegrationTestKeys {

  val testDocker: TaskKey[Unit] =
    taskKey[Unit](
      "Run Clio integration tests against a Clio instance and Elasticsearch cluster running in Docker."
    )

  val testDockerJenkins: TaskKey[Unit] =
    taskKey[Unit](
      "Run Clio integration tests against a Clio instance running in Docker and the dev Elasticsearch cluster."
    )

  val testDev: TaskKey[Unit] =
    taskKey[Unit](
      "Run Clio integration tests against the deployed Clio and Elasticsearch in dev."
    )

  val testStaging: TaskKey[Unit] =
    taskKey[Unit](
      "Run Clio integration tests against the deployed Clio and Elasticsearch in staging."
    )
}
