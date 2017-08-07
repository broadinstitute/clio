package org.broadinstitute.clio.sbt

import sbt._

/**
  * Library dependencies.
  */
object Dependencies {
  // The version numbers of each main and test dependency.
  private val AkkaHttpCirceVersion = "1.17.0"
  private val AkkaHttpVersion = "10.0.9"
  private val CirceVersion = "0.8.0"
  private val Elastic4sVersion = "5.4.6"
  private val EnumeratumVersion = "1.5.12"
  private val EnumeratumCirceVersion = "1.5.14"
  private val FicusVersion = "1.4.1"
  private val LogbackClassicVersion = "1.2.3"
  private val S_machStringVersion = "2.1.0"
  private val ScalaLoggingVersion = "3.7.1"
  private val ScalaTestVersion = "3.0.3"
  private val Slf4jVersion = "1.7.25"
  private val TestContainersScalaVersion = "0.6.0"

  /**
    * Version of scalafmt to pull in via plugin.
    * Declared separately from the other dependencies because it's
    * added through the `scalafmtVersion` key, not `libraryDependencies`.
    */
  val ScalafmtVersion = "1.1.0"

  /** Dependencies used in main code, and transitively by the test code. */
  val ServerMainDependencies: Seq[ModuleID] = Seq(
    "ch.qos.logback" % "logback-classic" % LogbackClassicVersion,
    "com.beachape" %% "enumeratum" % EnumeratumVersion,
    "com.beachape" %% "enumeratum-circe" % EnumeratumCirceVersion,
    "com.iheart" %% "ficus" % FicusVersion,
    "com.sksamuel.elastic4s" %% "elastic4s-circe" % Elastic4sVersion,
    "com.sksamuel.elastic4s" %% "elastic4s-http" % Elastic4sVersion,
    "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion,
    "com.typesafe.scala-logging" %% "scala-logging" % ScalaLoggingVersion,
    "de.heikoseeberger" %% "akka-http-circe" % AkkaHttpCirceVersion,
    "io.circe" %% "circe-core" % CirceVersion,
    "io.circe" %% "circe-generic" % CirceVersion,
    "io.circe" %% "circe-generic-extras" % CirceVersion,
    "io.circe" %% "circe-parser" % CirceVersion,
    "net.s_mach" %% "string" % S_machStringVersion,
    "org.slf4j" % "slf4j-api" % Slf4jVersion
  )

  /** Dependencies only used by test code. */
  private val ServerTestDependencies: Seq[ModuleID] = Seq(
    "com.dimafeng" %% "testcontainers-scala" % TestContainersScalaVersion,
    "com.typesafe.akka" %% "akka-http-testkit" % AkkaHttpVersion,
    "org.scalatest" %% "scalatest" % ScalaTestVersion
  ).map(_ % Test)

  /** The full list of server dependencies. */
  val ServerDependencies
    : Seq[ModuleID] = ServerMainDependencies ++ ServerTestDependencies

  val TransferModelDependencies: Seq[ModuleID] = Seq(
    "com.beachape" %% "enumeratum" % EnumeratumVersion
  )

  /** Suppress warning for evicted libraries. */
  val ServerOverrideDependencies: Set[ModuleID] = Set(
    "io.circe" %% "circe-core" % CirceVersion,
    "io.circe" %% "circe-generic" % CirceVersion,
    "io.circe" %% "circe-generic-extras" % CirceVersion,
    "io.circe" %% "circe-parser" % CirceVersion
  )
}
