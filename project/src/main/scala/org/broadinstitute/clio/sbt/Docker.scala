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
      /*
       * We need to use `entryPointShell` here to allow the environment variable to be substituted.
       * By default, using the shell form of ENTRYPOINT prevents signals from reaching the docker container.
       * Prepending `exec` fixes the problem.
       */
      entryPointShell("exec", "java", "$JAVA_OPTS", "-jar", artifactTargetPath)
    }
  }

  lazy val clientDockerFile: Initialize[Task[Dockerfile]] = Def.task {
    val artifact = assembly.value
    val artifactTargetPath = s"/app/${name.value}.jar"
    new Dockerfile {
      from("google/cloud-sdk:alpine")
      run("apk", "--update", "add", "openjdk8-jre")
      run("gcloud",
          "components",
          "install",
          "--quiet",
          "app-engine-java",
          "kubectl")
      label("CLIO_VERSION", version.value)
      add(artifact, artifactTargetPath)
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
    "elasticsearch" -> s"broadinstitute/elasticsearch:${Dependencies.ElasticsearchVersion}"
  )

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
