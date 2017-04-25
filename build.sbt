// ## sbt settings
// For more info, see: https://broadinstitute.atlassian.net/wiki/pages/viewpage.action?pageId=114531509

// Set the scala version used by the project. sbt version set in build.properties.
scalaVersion := "2.12.2"

// Project name
name := "clio"
// Project organization, stored in the assembly jar's META-INF/MANIFEST.MF
organization := "org.broadinstitute"
// This version number if this is a release commit, or the upcoming version number if this is a snapshot.
val ClioVersion = "0.0.1"

// Common version numbers of dependencies
val AkkaHttpVersion = "10.0.5"
val CirceVersion = "0.7.0"

// Dependencies
libraryDependencies ++= Seq(
  "com.iheart" %% "ficus" % "1.4.0",
  "org.slf4j" % "slf4j-api" % "1.7.25",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
  "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion,
  "io.circe" %% "circe-core" % CirceVersion,
  "io.circe" %% "circe-generic" % CirceVersion,
  "io.circe" %% "circe-parser" % CirceVersion,
  "de.heikoseeberger" %% "akka-http-circe" % "1.15.0",
  "com.typesafe.akka" %% "akka-http-testkit" % AkkaHttpVersion % Test,
  "org.scalatest" %% "scalatest" % "3.0.1" % Test
)

// Various compiler tweaks. More info available via:
//   https://blog.threatstack.com/useful-scalac-options-for-better-scala-development-part-1
//   sbt 'set scalacOptions in Compile += "-help"' compile
//   sbt 'set scalacOptions in Compile += "-X"' compile
//   sbt 'set scalacOptions in Compile += "-Y"' compile
val compilerSettings = Seq(
  "-Xlint",
  "-feature",
  "-Xmax-classfile-name", "200",
  "-target:jvm-1.8",
  "-encoding", "UTF-8",
  "-unchecked",
  "-deprecation",
  "-Xfuture",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-Ywarn-unused",
  "-Ywarn-unused-import",
  "-Xfatal-warnings"
)

// Since warnings are now errors, don't generate warnings for missing links, until someone discovers a way to fix links.
// http://stackoverflow.com/questions/31488335/scaladoc-2-11-6-fails-on-throws-tag-with-unable-to-find-any-member-to-link#31497874
val docSettings = Seq(
  "-no-link-warnings"
)

scalacOptions ++= compilerSettings
scalacOptions in (Compile, doc) ++= docSettings

// Write the version information into a configuration file
resourceGenerators in Compile += Def.task {
  val file = (resourceManaged in Compile).value / s"${name.value}-version.conf"
  val contents = Seq(s"${name.value}.version: ${version.value}")
  IO.writeLines(file, contents)
  Seq(file)
}.taskValue

// ## sbt-git settings

enablePlugins(GitVersioning)

git.baseVersion := ClioVersion

git.formattedShaVersion := {
  val hash = git.gitHeadCommit.value match {
    case Some(sha) => s"g${sha.take(7)}"
    case None => "UNKNOWN"
  }
  Option(s"${git.baseVersion.value}-$hash-SNAP")
}

// ## sbt-assembly settings

// Customize the executable jar name
assemblyJarName in assembly := s"${name.value}-${version.value}.jar"

// ## sbt-docker settings

enablePlugins(DockerPlugin)

// The name of docker organization
val dockerOrganization = "broadinstitute"

// The list of docker images to publish
imageNames in docker := Seq(
  // Sets a name with a tag that contains the project version
  ImageName(
    namespace = Some(dockerOrganization),
    repository = name.value,
    tag = Some("v" + version.value)
  )
)

// The Dockerfile
dockerfile in docker := {
  // The assembly task generates a fat JAR file
  val artifact: File = assembly.value
  val artifactTargetPath = s"/app/${artifact.name}"

  new Dockerfile {
    from("openjdk:8")
    expose(8080)
    add(artifact, artifactTargetPath)
    entryPoint("java", "-jar", artifactTargetPath)
  }
}

// sbt-docker customizations
buildOptions in docker := BuildOptions(
  cache = false,
  removeIntermediateContainers = BuildOptions.Remove.Always
)
