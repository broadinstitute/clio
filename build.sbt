import org.broadinstitute.clio.sbt._

// ## sbt settings
// For more info, see: https://broadinstitute.atlassian.net/wiki/pages/viewpage.action?pageId=114531509

enablePlugins(GitVersioning)

// Settings applied at the scope of the entire build.
inThisBuild(
  Seq(
    scalaVersion := Dependencies.ScalaVersion,
    organization := "org.broadinstitute",
    scalacOptions ++= Compilation.CompilerSettings,
    git.baseVersion := Versioning.clioBaseVersion.value,
    git.formattedShaVersion := Versioning.gitShaVersion.value,
    scalafmtVersion := Dependencies.ScalafmtVersion,
    scalafmtOnCompile := true,
    ignoreErrors in scalafmt := false,
    coverageHighlighting := false
  )
)

/*
 * Settings that must be applied to each project individually because
 * they are scoped to individual configurations / tasks, which don't
 * exist in the scope of ThisBuild.
 */
val commonSettings: Seq[Setting[_]] = Seq(
  scalacOptions in (Compile, doc) ++= Compilation.DocSettings,
  scalacOptions in (Compile, console) := Compilation.ConsoleSettings,
  resourceGenerators in Compile += Versioning.writeVersionConfig.taskValue,
  fork in run := true
)

val commonDockerSettings: Seq[Setting[_]] = Seq(
  assemblyJarName in assembly := Versioning.assemblyName.value,
  test in assembly := {},
  imageNames in docker := Docker.imageNames.value,
  buildOptions in docker := Docker.buildOptions.value
)

val commonTestDockerSettings: Seq[Setting[_]] = Seq(
  resourceGenerators in Test += Docker.writeTestImagesConfig.taskValue
)

/**
  * Root Clio project aggregating all sub-projects.
  * Enables running, i.e., "test" in the root project and
  * having unit tests runs across all sub-projects.
  */
lazy val clio = project
  .in(file("."))
  .settings(commonSettings)
  .aggregate(
    `clio-util`,
    `clio-transfer-model`,
    `clio-dataaccess-model`,
    `clio-server`,
    `clio-client`,
    `clio-integration-test`
  )
  .disablePlugins(AssemblyPlugin)

/**
  * Project holding:
  *   1. Generic shapeless utilities to map between types / fields / case classes.
  *   2. Generic types not really tied to any specific class of model, i.e. Location.
  *   3. Common configuration-parsing utilities.
  */
lazy val `clio-util` = project
  .settings(commonSettings)
  .enablePlugins(ArtifactoryPublishingPlugin)
  .disablePlugins(AssemblyPlugin)
  .settings(libraryDependencies ++= Dependencies.UtilDependencies)

/**
  * Project holding:
  *   1. Models describing the JSON inputs sent between the clio-client and clio-server.
  *   2. Generic circe utilities for mapping case classes into JSON / JSON schemas.
  */
lazy val `clio-transfer-model` = project
  .dependsOn(`clio-util`)
  .settings(commonSettings)
  .enablePlugins(ArtifactoryPublishingPlugin)
  .disablePlugins(AssemblyPlugin)
  .settings(libraryDependencies ++= Dependencies.TransferModelDependencies)

/**
  * Project holding:
  *   1. Models describing the JSON documents stored in elasticsearch by Clio.
  *   2. Auto-derived elasticsearch index models for our documents.
  */
lazy val `clio-dataaccess-model` = project
  .dependsOn(`clio-util`, `clio-transfer-model`)
  .settings(commonSettings)
  .disablePlugins(AssemblyPlugin)
  .settings(libraryDependencies ++= Dependencies.DataaccessModelDependencies)

/**
  * The main Clio web service, responsible for managing updates to metadata.
  */
lazy val `clio-server` = project
  .dependsOn(
    `clio-transfer-model`,
    `clio-dataaccess-model` % "compile->compile;test->test",
    `clio-util` % "compile->compile;test->test"
  )
  .settings(
    libraryDependencies ++= Dependencies.ServerDependencies,
    dependencyOverrides ++= Dependencies.ServerOverrideDependencies
  )
  .settings(commonSettings)
  .enablePlugins(DockerPlugin)
  .settings(commonDockerSettings)
  .settings(dockerfile in docker := Docker.serverDockerFile.value)
  .settings(commonTestDockerSettings)

/**
  * A CLP for communicating with the clio-server.
  */
lazy val `clio-client` = project
  .dependsOn(`clio-transfer-model`)
  .enablePlugins(
    ArtifactoryPublishingPlugin,
    ArtifactoryFatJarPublishingPlugin,
    DockerPlugin
  )
  .settings(libraryDependencies ++= Dependencies.ClientDependencies)
  .settings(commonSettings)
  .settings(commonTestDockerSettings)
  .settings(commonDockerSettings)
  .settings(dockerfile in docker := Docker.clientDockerFile.value)
  .settings(
    inConfig(FatJar) {
      addArtifact(
        Artifact("clio-client", "jar", "jar").copy(
          configurations = Seq(Default),
          classifier = Some("assembly")
        ),
        assembly
      )
    }
  )

/**
  * Integration tests for the clio-server and the clio-client.
  */
lazy val `clio-integration-test` = project
  .dependsOn(
    `clio-client`,
    `clio-dataaccess-model`,
    `clio-util` % "compile->compile;it->test"
  )
  .enablePlugins(ClioIntegrationTestPlugin)
  .disablePlugins(AssemblyPlugin)
  .settings(commonSettings)
  .settings(libraryDependencies ++= Dependencies.IntegrationTestDependencies)
