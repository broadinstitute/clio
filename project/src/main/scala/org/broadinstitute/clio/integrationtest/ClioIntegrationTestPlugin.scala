package org.broadinstitute.clio.integrationtest

import com.lucidchart.sbt.scalafmt.ScalafmtPlugin
import sbt._

/**
  * A plugin for creating an integration test task, that runs via docker.
  */
object ClioIntegrationTestPlugin extends AutoPlugin {

  /*
   * Ensure the scalafmt plugin loads before this, so we can
   * enable it for the IntegrationTest configuration.
   */
  override def requires: Plugins = ScalafmtPlugin

  /** Add our task to the project(s). */
  override val projectSettings: Seq[Setting[_]] =
    ClioIntegrationTestSettings.settings

  /** Add the IntegrationTest configuration to the project(s). */
  override val projectConfigurations: Seq[Configuration] = Seq(IntegrationTest)
}
