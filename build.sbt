import org.broadinstitute.clio.sbt._

// ## sbt settings
// For more info, see: https://broadinstitute.atlassian.net/wiki/pages/viewpage.action?pageId=114531509

enablePlugins(ClioIntegrationTestPlugin)
enablePlugins(DockerPlugin)
enablePlugins(GitVersioning)

// Set the scala version used by the project. sbt version set in build.properties.
scalaVersion := "2.12.2"
name := "clio"
organization := "org.broadinstitute"

libraryDependencies ++= Dependencies.ProjectDependencies
dependencyOverrides ++= Dependencies.OverrideDependencies

scalacOptions ++= Compilation.CompilerSettings
scalacOptions in(Compile, doc) ++= Compilation.DocSettings
scalacOptions in(Compile, console) := Compilation.ConsoleSettings

git.baseVersion := Versioning.ClioVersion
git.formattedShaVersion := Versioning.gitShaVersion.value
resourceGenerators in Compile += Versioning.writeVersionConfig.taskValue
assemblyJarName in assembly := Versioning.assemblyName.value

imageNames in docker := Docker.imageNames.value
dockerfile in docker := Docker.dockerFile.value
buildOptions in docker := Docker.buildOptions.value
