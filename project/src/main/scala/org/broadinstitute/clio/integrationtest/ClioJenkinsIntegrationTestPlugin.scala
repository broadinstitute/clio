package org.broadinstitute.clio.integrationtest

import sbt._

/**
  * A plugin for creating an integration test task, that runs via docker.
  */
object ClioJenkinsIntegrationTestPlugin extends AutoPlugin {

  /** The list of items automatically added to build.sbt, including the testDocker command. */
  object autoImport {

    /** The task key for running the integration tests. */
    val testDockerJenkins: TaskKey[Unit] =
      ClioJenkinsIntegrationTestSettings.testDockerJenkins
  }

  /** Add our task to the project(s). */
  override val projectSettings: Seq[Setting[Task[Unit]]] =
    ClioJenkinsIntegrationTestSettings.settings
}
