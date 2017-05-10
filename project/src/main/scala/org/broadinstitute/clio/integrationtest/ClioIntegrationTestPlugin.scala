package org.broadinstitute.clio.integrationtest

import sbt._

/**
  * A plugin for creating an integration test task, that runs via docker.
  */
object ClioIntegrationTestPlugin extends AutoPlugin {
  // The list of items automatically added to build.sbt, including the testDocker command.
  object autoImport {
    /** The task key for running the integration tests. */
    val testDocker = ClioIntegrationTestSettings.testDocker
  }

  // Automatically run our plugin with all requirements are satisfied.
  override def trigger = allRequirements

  // Add our task to the project(s)
  override val projectSettings = ClioIntegrationTestSettings.settings
}
