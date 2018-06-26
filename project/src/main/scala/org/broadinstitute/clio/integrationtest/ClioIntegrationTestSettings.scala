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

  /** Directory to which all temp files will be sent during Dockerized integration tests. */
  lazy val tmpTarget: Initialize[File] = Def.setting {
    target.value / "integration-test"
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
  private val AmbassadorImageId = "alpine/socat:latest"

  /** Java property used to override the default ambassador image in Testcontainers. */
  private val AmbassadorImageProp = "socat.container.image"

  /**
    * The output of `docker images` changed between version 1.X and version 17.X.
    *
    * In 1.X, images are prefixed with the repository name they came from, i.e.:
    *   docker.io/alpine/socat:latest
    *
    * In 17.X, the prefix is gone:
    *   alpine/socat:latest
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

  /** Variables to inject into integration-testkit code using sbt-buildinfo. */
  lazy val itKitBuildInfoKeys: Initialize[Seq[BuildInfoKey]] = Def.setting {
    Seq[BuildInfoKey](
      version,
      BuildInfoKey.constant(
        ("elasticsearchVersion", Dependencies.ElasticsearchVersion)
      )
    )
  }

  /** Variables to inject into integration-test code using sbt-buildinfo. */
  lazy val itBuildInfoKeys: Initialize[Seq[BuildInfoKey]] = Def.setting {
    Seq[BuildInfoKey](BuildInfoKey.constant(("tmpDir", tmpTarget.value.getAbsolutePath)))
  }

  /** Settings to add to the integration testkit. */
  lazy val testkitSettings: Seq[Setting[_]] = Seq(
    buildInfoKeys := itKitBuildInfoKeys.value,
    buildInfoPackage := s"${organization.value}.clio.integrationtest",
    buildInfoObject := "TestkitBuildInfo"
  )

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
        /*
         * We use sbt-buildinfo to inject parameters into test code.
         */
        buildInfoKeys := itBuildInfoKeys.value,
        buildInfoPackage := s"${organization.value}.clio.integrationtest",
        buildInfoObject := "IntegrationBuildInfo"
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
