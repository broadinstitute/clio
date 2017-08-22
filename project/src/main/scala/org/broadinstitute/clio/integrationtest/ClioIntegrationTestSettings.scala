package org.broadinstitute.clio.integrationtest

import org.broadinstitute.clio.sbt.{Compilation, Versioning}

import com.lucidchart.sbt.scalafmt.ScalafmtCorePlugin
import sbt._
import sbt.Def.{Initialize, Setting}
import sbt.Keys._

/**
  * Settings for running our integration tests.
  */
object ClioIntegrationTestSettings extends ClioIntegrationTestKeys {

  /** Directory to which all container logs will be sent during Dockerized integration tests. */
  lazy val itLogDir: Initialize[File] = Def.setting {
    target.value / "integration-test" / "logs"
  }

  /** Task to clear out the IT log dir before running tests. */
  lazy val resetItLogs: Initialize[Task[File]] = Def.task {
    val logDir = itLogDir.value
    IO.delete(logDir)
    IO.createDirectory(logDir)
    /*
     * Our Dockerized integration tests tail the clio log to check
     * for the "started" message before starting tests, so we ensure
     * it exists here.
     */
    val clioLog = logDir / "clio-server" / "clio.log"
    IO.touch(clioLog)
    clioLog
  }

  /** Environment variables to inject when running integration tests. */
  lazy val itEnvVars: Initialize[Task[Map[String, String]]] = Def.task {
    val logDir = itLogDir.value
    val confDir = (classDirectory in IntegrationTest).value / "org" / "broadinstitute" / "clio" / "integrationtest"

    Map(
      "CLIO_DOCKER_TAG" -> version.value,
      // TODO: Pull this version from elsewhere in the build.
      "ELASTICSEARCH_DOCKER_TAG" -> "5.4.0_6",
      "LOG_DIR" -> logDir.getAbsolutePath,
      "CONF_DIR" -> confDir.getAbsolutePath
    )
  }

  /** Settings to add to the project */
  lazy val settings: Seq[Setting[_]] = {

    Seq.concat(
      Defaults.itSettings,
      inConfig(IntegrationTest) {
        ScalafmtCorePlugin.autoImport.scalafmtSettings ++ Seq(
          scalacOptions in doc ++= Compilation.DocSettings,
          scalacOptions in console := Compilation.ConsoleSettings,
          resourceGenerators += Versioning.writeVersionConfig.taskValue
        )
      },
      Seq(
        /*
         * Override the top-level `test` definition to be a no-op so running "sbt test"
         * won't trigger integration tests along with the unit tests in other projects.
         *
         * Integration tests should be explicitly run with the keys defined below, or "it:test".
         */
        test := {},
        /*
         * Define aliases for "it:testOnly XYZ" to make running integration tests against
         * different setups / environments more convenient.
         */
        testDocker := (testOnly in IntegrationTest)
          .toTask(" *FullDockerIntegrationSpec")
          .value,
        testDockerJenkins := (testOnly in IntegrationTest)
          .toTask(" *DockerDevIntegrationSpec")
          .value,
        testDev := (testOnly in IntegrationTest)
          .toTask(" *DevEnvIntegrationSpec")
          .value,
        testStaging := (testOnly in IntegrationTest)
          .toTask(" *StagingEnvIntegrationSpec")
          .value,
        /*
         * Testcontainers registers a JVM shutdown hook to remove the containers
         * it creates using docker-compose. Forking when running integration tests
         * makes all containers be removed as soon as tests finish running.
         *
         * Forking also allows us to set environment variables for substitution
         * in our docker-compose files.
         */
        fork in IntegrationTest := true,
        envVars in IntegrationTest ++= itEnvVars.value,
        resourceGenerators in IntegrationTest +=
          resetItLogs.map(Seq(_)).taskValue
      )
    )
  }
}
