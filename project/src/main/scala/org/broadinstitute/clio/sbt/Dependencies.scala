package org.broadinstitute.clio.sbt

import sbt._

/**
  * Library dependencies.
  */
object Dependencies {
  // The version numbers of each main and test dependency.
  private val AkkaHttpCirceVersion = "1.17.0"
  private val AkkaHttpVersion = "10.0.10"
  private val AlpakkaVersion = "0.14"
  private val CaseAppVersion = "2.0.0-M1"
  private val CirceVersion = "0.8.0"
  private val Elastic4sVersion = "5.4.13"
  private val EnumeratumVersion = "1.5.12"
  private val EnumeratumCirceVersion = "1.5.14"
  private val FicusVersion = "1.4.3"
  private val GoogleAuthHttpVersion = "0.7.1"
  private val GoogleCloudNioVersion = "0.26.0-alpha"
  private val JimfsVersion = "1.1"
  private val LogbackClassicVersion = "1.2.3"
  private val S_machStringVersion = "2.1.0"
  private val ScalaLoggingVersion = "3.7.2"
  private val ScalaTestVersion = "3.0.4"
  private val ShapelessVersion = "2.3.2"
  private val Slf4jVersion = "1.7.25"
  private val SwaggerUi = "3.1.5"
  private val TestContainersScalaVersion = "0.7.0"
  private val TypesafeConfigVersion = "1.3.2"
  private val UUIDVersion = "3.1.3"
  private val VaultJavaDriverVersion = "3.0.0"

  /** Version of Scala to build Clio with. */
  val ScalaVersion = "2.12.4"

  /**
    * Version of scalafmt to pull in via plugin.
    * Declared separately from the other dependencies because it's
    * added through the `scalafmtVersion` key, not `libraryDependencies`.
    */
  val ScalafmtVersion = "1.3.0"

  /** Version of our Elasticsearch Docker image to use during integration tests. */
  val ElasticsearchVersion = "5.4.0_6"

  /** Dependencies used in main code, and transitively by the test code. */
  val ServerMainDependencies: Seq[ModuleID] = Seq(
    "ch.qos.logback" % "logback-classic" % LogbackClassicVersion,
    "com.google.cloud" % "google-cloud-nio" % GoogleCloudNioVersion,
    "com.lightbend.akka" %% "akka-stream-alpakka-file" % AlpakkaVersion,
    "com.sksamuel.elastic4s" %% "elastic4s-http" % Elastic4sVersion,
    "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion,
    "com.typesafe.scala-logging" %% "scala-logging" % ScalaLoggingVersion,
    "de.heikoseeberger" %% "akka-http-circe" % AkkaHttpCirceVersion,
    "io.circe" %% "circe-core" % CirceVersion,
    "io.circe" %% "circe-generic" % CirceVersion,
    "io.circe" %% "circe-generic-extras" % CirceVersion,
    "io.circe" %% "circe-parser" % CirceVersion,
    "org.slf4j" % "slf4j-api" % Slf4jVersion,
    "org.webjars" % "swagger-ui" % SwaggerUi
  )

  /** Dependencies only used by test code. */
  private val ServerTestDependencies: Seq[ModuleID] = Seq(
    "com.dimafeng" %% "testcontainers-scala" % TestContainersScalaVersion,
    "com.google.jimfs" % "jimfs" % JimfsVersion,
    "com.typesafe.akka" %% "akka-http-testkit" % AkkaHttpVersion,
    "org.scalatest" %% "scalatest" % ScalaTestVersion
  ).map(_ % Test)

  /** The full list of server dependencies. */
  val ServerDependencies
    : Seq[ModuleID] = ServerMainDependencies ++ ServerTestDependencies

  /** Suppress warning for evicted libraries. */
  val ServerOverrideDependencies: Set[ModuleID] = Set(
    "io.circe" %% "circe-core" % CirceVersion,
    "io.circe" %% "circe-generic" % CirceVersion,
    "io.circe" %% "circe-generic-extras" % CirceVersion,
    "io.circe" %% "circe-parser" % CirceVersion
  )

  val ClientMainDependencies: Seq[ModuleID] = Seq(
    "ch.qos.logback" % "logback-classic" % LogbackClassicVersion,
    "com.github.alexarchambault" %% "case-app" % CaseAppVersion,
    "com.iheart" %% "ficus" % FicusVersion,
    "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion,
    "com.typesafe.scala-logging" %% "scala-logging" % ScalaLoggingVersion,
    "de.heikoseeberger" %% "akka-http-circe" % AkkaHttpCirceVersion,
    "io.circe" %% "circe-core" % CirceVersion,
    "io.circe" %% "circe-generic" % CirceVersion,
    "io.circe" %% "circe-generic-extras" % CirceVersion,
    "io.circe" %% "circe-parser" % CirceVersion,
    "org.slf4j" % "slf4j-api" % Slf4jVersion
  )

  private val ClientTestDependencies: Seq[ModuleID] = Seq(
    "com.typesafe.akka" %% "akka-http-testkit" % AkkaHttpVersion,
    "org.scalatest" %% "scalatest" % ScalaTestVersion
  ).map(_ % Test)

  /**The full list of client dependencies. */
  val ClientDependencies
    : Seq[ModuleID] = ClientMainDependencies ++ ClientTestDependencies

  val UtilMainDependencies: Seq[ModuleID] = Seq(
    "com.beachape" %% "enumeratum" % EnumeratumVersion,
    "com.beachape" %% "enumeratum-circe" % EnumeratumCirceVersion,
    "com.chuusai" %% "shapeless" % ShapelessVersion,
    "com.fasterxml.uuid" % "java-uuid-generator" % UUIDVersion,
    "com.google.auth" % "google-auth-library-oauth2-http" % GoogleAuthHttpVersion,
    "com.iheart" %% "ficus" % FicusVersion,
    "com.typesafe" % "config" % TypesafeConfigVersion,
    "com.typesafe.scala-logging" %% "scala-logging" % ScalaLoggingVersion,
    "io.circe" %% "circe-core" % CirceVersion,
    "io.circe" %% "circe-parser" % CirceVersion,
    "io.circe" %% "circe-generic" % CirceVersion,
    "io.circe" %% "circe-generic-extras" % CirceVersion,
    "net.s_mach" %% "string" % S_machStringVersion,
    "org.scala-lang" % "scala-reflect" % ScalaVersion
  )
  private val UtilTestDependencies: Seq[ModuleID] = Seq(
    "org.scalatest" %% "scalatest" % ScalaTestVersion
  ).map(_ % Test)

  val UtilDependencies: Seq[ModuleID] =
    UtilMainDependencies ++ UtilTestDependencies

  val TransferModelMainDependencies: Seq[ModuleID] = Seq(
    "com.beachape" %% "enumeratum-circe" % EnumeratumCirceVersion,
    "io.circe" %% "circe-core" % CirceVersion,
    "io.circe" %% "circe-generic" % CirceVersion,
    "io.circe" %% "circe-generic-extras" % CirceVersion,
    "io.circe" %% "circe-parser" % CirceVersion
  )
  private val TransferModelTestDependencies: Seq[ModuleID] = Seq(
    "org.scalatest" %% "scalatest" % ScalaTestVersion
  ).map(_ % Test)

  val TransferModelDependencies
    : Seq[ModuleID] = TransferModelMainDependencies ++ TransferModelTestDependencies

  val DataaccessModelMainDependencies: Seq[ModuleID] = Seq(
    "com.fasterxml.uuid" % "java-uuid-generator" % UUIDVersion,
    "com.sksamuel.elastic4s" %% "elastic4s-circe" % Elastic4sVersion,
    "com.sksamuel.elastic4s" %% "elastic4s-http" % Elastic4sVersion
  )
  private val DataaccessModelTestDependencies: Seq[ModuleID] = Seq(
    "org.scalatest" %% "scalatest" % ScalaTestVersion
  ).map(_ % Test)

  val DataaccessModelDependencies: Seq[ModuleID] =
    DataaccessModelMainDependencies ++ DataaccessModelTestDependencies

  val IntegrationTestDependencies: Seq[ModuleID] = Seq(
    "ch.qos.logback" % "logback-classic" % LogbackClassicVersion,
    "com.bettercloud" % "vault-java-driver" % VaultJavaDriverVersion,
    "com.dimafeng" %% "testcontainers-scala" % TestContainersScalaVersion,
    "com.fasterxml.uuid" % "java-uuid-generator" % UUIDVersion,
    "com.google.cloud" % "google-cloud-nio" % GoogleCloudNioVersion,
    "com.lightbend.akka" %% "akka-stream-alpakka-file" % AlpakkaVersion,
    "com.sksamuel.elastic4s" %% "elastic4s-http" % Elastic4sVersion,
    "com.sksamuel.elastic4s" %% "elastic4s-circe" % Elastic4sVersion,
    "com.typesafe.akka" %% "akka-http-testkit" % AkkaHttpVersion,
    "com.typesafe.scala-logging" %% "scala-logging" % ScalaLoggingVersion,
    "de.heikoseeberger" %% "akka-http-circe" % AkkaHttpCirceVersion,
    "org.scalatest" %% "scalatest" % ScalaTestVersion,
    "org.slf4j" % "slf4j-api" % Slf4jVersion
  ).map(_ % IntegrationTest)
}
