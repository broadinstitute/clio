package org.broadinstitute.clio.integrationtest

import sbt.Def.{Initialize, Setting}
import sbt.Keys._
import sbt._

/**
  * Settings definitions for the plugin.
  */
object ClioIntegrationTestSettings extends AutoPlugin {

  /** The task key for running the integration tests. */
  val testDocker: TaskKey[Unit] =
    taskKey[Unit]("Run clio integration tests via docker.")

  /** The actual task to run docker. */
  lazy val runTestDocker: Initialize[Task[Unit]] = Def.task {
    // Use the settings classDirectory for tests, and the project version
    val testClassesDirectory = (classDirectory in Test).value
    val clioVersion = version.value

    // Wait for task test:compile, and the task to create the logs
    (compile in Test).value
    val log = streams.value.log

    new ClioIntegrationTestRunner(testClassesDirectory, clioVersion, log).run()
  }

  /** Settings to add to the project */
  lazy val settings: Seq[Setting[Task[Unit]]] = Seq(
    testDocker := runTestDocker.value)
}
