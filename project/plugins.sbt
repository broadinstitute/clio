// For more info on these plugins, see https://broadinstitute.atlassian.net/wiki/pages/viewpage.action?pageId=114531509

val SbtAssemblyVersion = "0.14.4"
val SbtGitVersion = "0.9.3"
val SbtDockerVersion = "1.4.1"
val ScalafmtVersion = "1.7"
val Slf4jVersion = "1.7.25"
val TypesafeConfigVersion = "1.3.1"

addSbtPlugin("com.typesafe.sbt" % "sbt-git" % SbtGitVersion)
libraryDependencies += "org.slf4j" % "slf4j-nop" % Slf4jVersion

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % SbtAssemblyVersion)

addSbtPlugin("se.marcuslonnberg" % "sbt-docker" % SbtDockerVersion)

addSbtPlugin("com.lucidchart" % "sbt-scalafmt" % ScalafmtVersion)

// Various compiler tweaks for our ClioIntegrationTestPlugin.
// More info available via:
//   https://tpolecat.github.io/2014/04/11/scalac-flags.html
//   https://blog.threatstack.com/useful-scalac-options-for-better-scala-development-part-1
//   sbt 'set scalacOptions in Compile += "-help"' compile
//   sbt 'set scalacOptions in Compile += "-X"' compile
//   sbt 'set scalacOptions in Compile += "-Y"' compile
scalacOptions ++= Seq(
  "-Xlint",
  "-feature",
  "-Xmax-classfile-name",
  "200",
  "-encoding",
  "UTF-8",
  "-unchecked",
  "-deprecation",
  "-Xfuture",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-Xfatal-warnings"
)
libraryDependencies += "com.typesafe" % "config" % TypesafeConfigVersion

scalafmtVersion in ThisBuild := "1.0.0-RC4"
scalafmtOnCompile in ThisBuild := true
ignoreErrors in (ThisBuild, scalafmt) := false
