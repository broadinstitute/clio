package org.broadinstitute.clio.sbt

import sbt.Def.Initialize
import sbt.Keys._
import sbt._
import sbtassembly.AssemblyKeys._
import sbtdocker._

/**
  * Customizations for building docker images via sbt-docker.
  */
object Docker {
  /** The name of docker organization. */
  private val DockerOrganization = "broadinstitute"

  /** The list of docker images to publish. */
  lazy val imageNames: Initialize[Task[Seq[ImageName]]] = Def.task {
    Seq(
      // Sets a name with a tag that contains the project version
      ImageName(
        namespace = Option(DockerOrganization),
        repository = name.value,
        tag = Option(version.value)
      )
    )
  }

  /** The Dockerfile. */
  lazy val dockerFile: Initialize[Task[Dockerfile]] = Def.task {
    // The assembly task generates a fat JAR file
    val artifact = assembly.value
    val artifactTargetPath = "/app/clio.jar"

    new Dockerfile {
      from("openjdk:8")
      label("CLIO_VERSION", version.value)
      expose(8080)
      add(artifact, artifactTargetPath)
      entryPoint("java", "-jar", artifactTargetPath)
    }
  }

  /** Other customizations for sbt-docker. */
  lazy val buildOptions: Initialize[BuildOptions] = Def.setting {
    BuildOptions(
      cache = false,
      removeIntermediateContainers = BuildOptions.Remove.Always
    )
  }
}
