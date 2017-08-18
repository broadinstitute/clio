package org.broadinstitute.clio.integrationtest

import sbt._

/**
  * A plugin for creating an integration test task, that runs via docker.
  */
object ClioIntegrationTestPlugin extends AutoPlugin {

  /** The list of items automatically added to build.sbt, including the testDocker command. */
  object autoImport extends ClioIntegrationTestKeys {}

  /** Add our task to the project(s). */
  override val projectSettings: Seq[Setting[_]] =
    ClioIntegrationTestSettings.settings

  /** Add the IntegrationTest configuration to the project(s). */
  override val projectConfigurations: Seq[Configuration] = Seq(IntegrationTest)
}
