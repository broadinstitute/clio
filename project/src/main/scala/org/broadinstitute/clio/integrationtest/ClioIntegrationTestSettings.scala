package org.broadinstitute.clio.integrationtest

import org.broadinstitute.clio.sbt.{Compilation, Dependencies, Versioning}

import com.lucidchart.sbt.scalafmt.ScalafmtCorePlugin
import sbt._
import sbt.Def.{Initialize, Setting}
import sbt.Keys._

/**
  * Settings for running our integration tests.
  */
object ClioIntegrationTestSettings {

  /** Directory to which all container logs will be sent during Dockerized integration tests. */
  lazy val logTarget: Initialize[File] = Def.setting {
    target.value / "integration-test" / "logs"
  }

  /** Directory to which Clio will write metadata updates during Dockerized integration tests. */
  lazy val persistenceTarget: Initialize[File] = Def.setting {
    target.value / "integration-test" / "persistence"
  }

  /** File to which Clio-server container logs will be sent during Dockerized integration tests. */
  lazy val clioLogFile: Initialize[File] = Def.setting {
    logTarget.value / "clio-server" / "clio.log"
  }

  /** Task to clear out the IT log and persistence dirs before running tests. */
  lazy val resetLogs: Initialize[Task[File]] = Def.task {
    val logDir = logTarget.value
    val persistenceDir = persistenceTarget.value

    IO.delete(logDir)
    IO.createDirectory(logDir)
    IO.delete(persistenceDir)
    IO.createDirectory(persistenceDir)

    /*
     * Our Dockerized integration tests tail the clio log to check
     * for the "started" message before starting tests, so we ensure
     * it exists here.
     */
    val clioLog = clioLogFile.value
    IO.touch(clioLog)
    clioLog
  }

  /** Environment variables to inject when running integration tests. */
  lazy val itEnvVars: Initialize[Task[Map[String, String]]] = Def.task {
    val logDir = logTarget.value
    val clioLog = clioLogFile.value
    val persistenceDir = persistenceTarget.value
    val confDir = (classDirectory in IntegrationTest).value / "org" / "broadinstitute" / "clio" / "integrationtest"

    Map(
      "CLIO_DOCKER_TAG" -> version.value,
      "ELASTICSEARCH_DOCKER_TAG" -> Dependencies.ElasticsearchVersion,
      "LOG_DIR" -> logDir.getAbsolutePath,
      "CLIO_LOG_FILE" -> clioLog.getAbsolutePath,
      "CONF_DIR" -> confDir.getAbsolutePath,
      "LOCAL_PERSISTENCE_DIR" -> persistenceDir.getAbsolutePath
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
         * Integration tests should be explicitly run with the keys defined below,
         * or "it:test" / "it:testOnly XYZ".
         */
        test := {},
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
          resetLogs.map(Seq(_)).taskValue
      )
    )
  }
}
