// For more info on these plugins, see https://broadinstitute.atlassian.net/wiki/pages/viewpage.action?pageId=114531509

addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "0.9.2")
libraryDependencies += "org.slf4j" % "slf4j-nop" % "1.7.25"

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.4")

addSbtPlugin("se.marcuslonnberg" % "sbt-docker" % "1.4.1")

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
  "-Xmax-classfile-name", "200",
  "-encoding", "UTF-8",
  "-unchecked",
  "-deprecation",
  "-Xfuture",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-Xfatal-warnings"
)
libraryDependencies += "com.typesafe" % "config" % "1.3.1"
