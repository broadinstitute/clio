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
  .aggregate(
    `clio-util`,
    `clio-transfer-model`,
    `clio-dataaccess-model`,
    `clio-server`,
    `clio-client`,
    `clio-integration-test`
  )
  .disablePlugins(AssemblyPlugin)

lazy val `clio-util` = project
  .settings(commonSettings)
  .disablePlugins(AssemblyPlugin)
  .settings(libraryDependencies ++= Dependencies.UtilDependencies)

lazy val `clio-transfer-model` = project
  .dependsOn(`clio-util`)
  .settings(commonSettings)
  .disablePlugins(AssemblyPlugin)
  .settings(libraryDependencies ++= Dependencies.TransferModelDependencies)

lazy val `clio-dataaccess-model` = project
  .dependsOn(`clio-util`)
  .settings(commonSettings)
  .disablePlugins(AssemblyPlugin)
  .settings(libraryDependencies ++= Dependencies.DataaccessModelDependencies)

lazy val `clio-server` = project
  .dependsOn(`clio-transfer-model`, `clio-dataaccess-model`)
  .settings(
    libraryDependencies ++= Dependencies.ServerDependencies,
    dependencyOverrides ++= Dependencies.ServerOverrideDependencies
  )
  .settings(commonSettings)
  .enablePlugins(DockerPlugin)
  .settings(commonDockerSettings)
  .settings(dockerfile in docker := Docker.serverDockerFile.value)
  .settings(commonTestDockerSettings)

lazy val `clio-client` = project
  .dependsOn(`clio-transfer-model`)
  .settings(libraryDependencies ++= Dependencies.ClientDependencies)
  .settings(commonSettings)
  .settings(commonTestDockerSettings)

lazy val `clio-integration-test` = project
  .enablePlugins(DockerPlugin)
  .settings(commonDockerSettings)
  .settings(dockerfile in docker := Docker.integrationTestDockerFile.value)
  .enablePlugins(ClioIntegrationTestPlugin)
  .settings(commonTestDockerSettings)

addCommandAlias(
  "testCoverage",
  "; clean; coverage; test; coverageOff; coverageReport; coverageAggregate"
)
