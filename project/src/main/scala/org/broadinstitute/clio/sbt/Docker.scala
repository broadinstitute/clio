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
  lazy val serverDockerFile: Initialize[Task[Dockerfile]] = Def.task {
    // The assembly task generates a fat JAR file
    val artifact = assembly.value
    val artifactTargetPath = s"/app/${name.value}.jar"

    new Dockerfile {
      from("openjdk:8")
      label("CLIO_VERSION", version.value)
      expose(8080)
      add(artifact, artifactTargetPath)
      entryPoint("java", "-jar", artifactTargetPath)
    }
  }

  /** The Dockerfile. */
  lazy val integrationTestDockerFile: Initialize[Task[Dockerfile]] = Def.task {
    val pythonSource = baseDirectory(_ / "src" / "main" / "python").value
    new Dockerfile {

      // TODO: Publish the pytest Dockerfile to Docker Hub, then reference the pushed image here.
      // Generic pytest docker image
      from("python:3.6")
      run("mkdir", "/app")
      workDir("/app")
      copy(pythonSource / "requirements.txt", "./")
      run("pip", "install", "-r", "requirements.txt")
      entryPoint("pytest", "-vv", "-s")

      // clio-integration-test specifics
      label("CLIO_VERSION", version.value)
      workDir("/cliointegrationtest")
      copy(pythonSource / "clio_test.py", "./")
      cmd("/cliointegrationtest/clio_test.py")

    }
  }

  /** Other customizations for sbt-docker. */
  lazy val buildOptions: Initialize[BuildOptions] = Def.setting {
    BuildOptions(
      cache = false,
      removeIntermediateContainers = BuildOptions.Remove.Always
    )
  }

  /** The name of the test images. */
  private val TestImages = Map(
    "elasticsearch" -> "broadinstitute/elasticsearch:5.4.0_6")

  /** Write the version information into a configuration file. */
  lazy val writeTestImagesConfig: Initialize[Task[Seq[File]]] = Def.task {
    val file = (resourceManaged in Test).value / "clio-docker-images.conf"
    val contents = TestImages.toSeq map {
      case (imageName, imageId) => s"""clio.docker.$imageName: "$imageId""""
    }
    IO.writeLines(file, contents)
    Seq(file)
  }
}
