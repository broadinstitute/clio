package org.broadinstitute.clio.integrationtest

import com.lucidchart.sbt.scalafmt.ScalafmtCorePlugin
import org.broadinstitute.clio.sbt.{Compilation, Dependencies, Versioning}
import sbt.Def.{Initialize, Setting}
import sbt.Keys._
import sbt._
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

  /** Files to which Elasticsearch container logs will be sent during Dockerized integration tests. */
  lazy val elasticsearchLogFiles: Initialize[Seq[File]] = Def.setting {
    val logBase = logTarget.value
    Seq("elasticsearch1", "elasticsearch2").flatMap { container =>
      Seq(
        logBase / container / "docker-cluster.log",
        logBase / container / "docker-cluster_access.log"
      )
    }
  }

  /** Task to clear out the IT log and persistence dirs before running tests. */
  lazy val resetLogs: Initialize[Task[Unit]] = Def.task {
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
     *
     * We also touch and chmod the Elasticsearch log files to work
     * around docker-compose behavior on Jenkins. Elasticsearch runs
     * inside its container as the user / group "elasticsearch", but
     * on Jenkins docker-compose mounts the files (by default) with
     * uid:gid "1023:1025". We set the logs to be world-readable and
     * world-writeable to work around this and avoid permissions
     * errors in log4j.
     */
    val logs = clioLogFile.value +: elasticsearchLogFiles.value
    logs.foreach { f =>
      IO.touch(f)
      f.setReadable(true, false)
      f.setWritable(true, false)
    }
  }

  /**
    * Regex for parsing major version out of "docker --version" stdout.
    *
    * Example stdout:
    *
    *   Docker version 17.09.0-ce, build afdb6d4
    */
  private val DockerMajorVersionRegex = ".+version\\s+(\\d+)\\..+".r

  /**
    * Name of the properties file used for overriding testcontainers properties.
    *
    * Testcontainers-java looks for this specific filename on the classpath, so
    * it can't be changed to anything else.
    */
  private val TestcontainersPropsFile = "testcontainers.properties"

  /** Organization, name, and version for the "ambassador" image used by Testcontainers. */
  private val AmbassadorImageId = "richnorth/ambassador:latest"

  /** Java property used to override the default ambassador image in Testcontainers. */
  private val AmbassadorImageProp = "ambassador.container.image"

  /**
    * The output of `docker images` changed between version 1.X and version 17.X.
    *
    * In 1.X, images are prefixed with the repository name they came from, i.e.:
    *   docker.io/richnorth/ambassador:latest
    *
    * In 17.X, the prefix is gone:
    *   richnorth/ambassador:latest
    *
    * Testcontainers-java can only handle the latter by default, but it exposes a
    * way to override its built-in image names via properties file. We override the
    * name of the "ambassador" image, which is used to send HTTP requests to
    * services running in a test network set up by docker-compose.
    */
  lazy val overrideTestcontainersAmbassador: Initialize[Task[Seq[File]]] =
    Def.task {
      val generatedResourceDir = (resourceManaged in IntegrationTest).value

      val DockerMajorVersionRegex(dockerMajorVersion) =
        Process(Seq("docker", "--version")).!!.trim

      val propertiesFile = generatedResourceDir / TestcontainersPropsFile
      val imagePrefix = if (dockerMajorVersion.toInt == 1) {
        "docker.io/"
      } else {
        ""
      }

      IO.write(
        propertiesFile,
        s"$AmbassadorImageProp=$imagePrefix$AmbassadorImageId"
      )
      Seq(propertiesFile)
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
        resourceGenerators ++= Seq(
          Versioning.writeVersionConfig.taskValue,
          overrideTestcontainersAmbassador.taskValue
        ),
        /*
         * Testcontainers registers a JVM shutdown hook to remove the containers
         * it creates using docker-compose. Forking when running integration tests
         * makes all containers be removed as soon as tests finish running.
         */
        fork := true,
        // Reset log files before running any tests.
        test := test.dependsOn(resetLogs).value,
        testOnly := testOnly.dependsOn(resetLogs).evaluated,
        testQuick := testQuick.dependsOn(resetLogs).evaluated,
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
