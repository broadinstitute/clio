import org.broadinstitute.clio.sbt._

// ## sbt settings
// For more info, see: https://broadinstitute.atlassian.net/wiki/pages/viewpage.action?pageId=114531509

enablePlugins(GitVersioning)

// Settings applied at the scope of the entire build.
inThisBuild(
  Seq(
    scalaVersion := "2.12.2",
    organization := "org.broadinstitute",
    scalacOptions ++= Compilation.CompilerSettings,
    git.baseVersion := Versioning.clioBaseVersion.value,
    git.formattedShaVersion := Versioning.gitShaVersion.value
  )
)

// Settings that must be applied to each project because they are scoped
// to individual configurations / tasks.
val commonSettings: Seq[Setting[_]] = Seq(
  scalacOptions in (Compile, doc) ++= Compilation.DocSettings,
  scalacOptions in (Compile, console) := Compilation.ConsoleSettings,
  resourceGenerators in Compile += Versioning.writeVersionConfig.taskValue
)

val commonDockerSettings: Seq[Setting[_]] = Seq(
  assemblyJarName in assembly := Versioning.assemblyName.value,
  imageNames in docker := Docker.imageNames.value,
  buildOptions in docker := Docker.buildOptions.value
)

val commonTestDockerSettings: Seq[Setting[_]] = Seq(
  resourceGenerators in Test += Docker.writeTestImagesConfig.taskValue
)

lazy val clio = project
  .in(file("."))
  .settings(commonSettings)
  .aggregate(`clio-integration-test`, `clio-transfer-model`, `clio-server`)
  .disablePlugins(AssemblyPlugin)

lazy val `clio-integration-test` = project
  .settings(commonSettings)
  .enablePlugins(DockerPlugin)
  .settings(commonDockerSettings)
  .settings(dockerfile in docker := Docker.integrationTestDockerFile.value)
  .enablePlugins(ClioIntegrationTestPlugin)
  .settings(commonTestDockerSettings)

lazy val `clio-transfer-model` = project
  .settings(libraryDependencies ++= Dependencies.TransferModelDependencies)
  .settings(commonSettings)
  .disablePlugins(AssemblyPlugin)

lazy val `clio-server` = project
  .dependsOn(`clio-transfer-model`)
  .settings(
    libraryDependencies ++= Dependencies.ServerDependencies,
    dependencyOverrides ++= Dependencies.ServerOverrideDependencies
  )
  .settings(commonSettings)
  .enablePlugins(DockerPlugin)
  .settings(commonDockerSettings)
  .settings(dockerfile in docker := Docker.serverDockerFile.value)
  .settings(commonTestDockerSettings)

val ScalafmtVersion = "1.1.0"

scalafmtVersion in ThisBuild := ScalafmtVersion
scalafmtOnCompile in ThisBuild := true
ignoreErrors in (ThisBuild, scalafmt) := false
