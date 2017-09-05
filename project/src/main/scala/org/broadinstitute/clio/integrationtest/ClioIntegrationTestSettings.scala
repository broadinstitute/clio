package org.broadinstitute.clio.integrationtest

import org.broadinstitute.clio.sbt.{Compilation, Dependencies, Versioning}

import com.lucidchart.sbt.scalafmt.ScalafmtCorePlugin
import sbt._
import sbt.Def.{Initialize, Setting}
import sbt.Keys._
import sbtbuildinfo.{BuildInfoKey, BuildInfoKeys, BuildInfoPlugin}

/**
  * Settings for running our integration tests.
  */
object ClioIntegrationTestSettings extends BuildInfoKeys {

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

  /** Variables to inject into integration-test code using sbt-buildinfo. */
  lazy val itBuildInfoKeys: Initialize[Seq[BuildInfoKey]] = Def.setting {
    val confDir = (classDirectory in IntegrationTest).value / "org" / "broadinstitute" / "clio" / "integrationtest"

    Seq[BuildInfoKey](
      version,
      BuildInfoKey.constant(
        ("elasticsearchVersion", Dependencies.ElasticsearchVersion)
      ),
      BuildInfoKey.constant(("logDir", logTarget.value.getAbsolutePath)),
      BuildInfoKey.constant(("clioLog", clioLogFile.value.getAbsolutePath)),
      BuildInfoKey.constant(("confDir", confDir.getAbsolutePath)),
      BuildInfoKey.constant(
        ("persistenceDir", persistenceTarget.value.getAbsolutePath)
      )
    )
  }

  /** Settings to add to the project */
  lazy val settings: Seq[Setting[_]] = {

    val itSettings = Seq.concat(
      ScalafmtCorePlugin.autoImport.scalafmtSettings,
      BuildInfoPlugin.buildInfoScopedSettings(IntegrationTest),
      Seq(
        scalacOptions in doc ++= Compilation.DocSettings,
        scalacOptions in console := Compilation.ConsoleSettings,
        resourceGenerators += Versioning.writeVersionConfig.taskValue,
        /*
         * Testcontainers registers a JVM shutdown hook to remove the containers
         * it creates using docker-compose. Forking when running integration tests
         * makes all containers be removed as soon as tests finish running.
         */
        fork := true,
        resourceGenerators +=
          resetLogs.map(Seq(_)).taskValue,
        /*
         * We use sbt-buildinfo to inject parameters into test code.
         */
        buildInfoKeys := itBuildInfoKeys.value,
        buildInfoPackage := s"${organization.value}.clio.integrationtest",
        buildInfoObject := "ClioBuildInfo"
      )
    )

    Seq.concat(
      Defaults.itSettings,
      inConfig(IntegrationTest)(itSettings),
      Seq(
        /*
         * Override the top-level `test` definition to be a no-op so running "sbt test"
         * won't trigger integration tests along with the unit tests in other projects.
         *
         * Integration tests should be explicitly run with the keys defined below,
         * or "it:test" / "it:testOnly XYZ".
         */
        test := {}
      )
    )
  }
}
