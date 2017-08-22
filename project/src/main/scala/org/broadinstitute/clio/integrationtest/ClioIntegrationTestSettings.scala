package org.broadinstitute.clio.integrationtest

import org.broadinstitute.clio.sbt.{Compilation, Dependencies, Versioning}

import com.bettercloud.vault.{Vault, VaultConfig}
import com.google.auth.oauth2.ServiceAccountCredentials
import com.lucidchart.sbt.scalafmt.ScalafmtCorePlugin
import sbt._
import sbt.Def.{Initialize, Setting}
import sbt.Keys._

import scala.collection.JavaConverters._

/**
  * Settings for running our integration tests.
  */
object ClioIntegrationTestSettings {

  /** URL of vault server to use when getting bearer tokens for service accounts. */
  private val vaultUrl = "https://clotho.broadinstitute.org:8200/"

  /** List of possible token-file locations, in order of preference. */
  private val vaultTokenFiles = Seq(
    new File("/etc/vault-token-dsde"),
    new File(s"${System.getProperty("user.home")}/.vault-token")
  )

  /**
    * Path within Vault to the service account info Jenkins should use when talking to Clio.
    *
    * TODO: Once we set up a service account for Clio itself, we should use that account.
    */
  private val vaultPath = "secret/dsde/gotc/dev/picard/picard-account.pem"

  /** Scopes needed from Google to get past Clio's auth proxy. */
  private val authScopes = Seq(
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/userinfo.email"
  )

  /** Directory to which all container logs will be sent during Dockerized integration tests. */
  lazy val logTarget: Initialize[File] = Def.setting {
    target.value / "integration-test" / "logs"
  }

  /** File to which Clio-server container logs will be sent during Dockerized integration tests. */
  lazy val clioLogFile: Initialize[File] = Def.setting {
    logTarget.value / "clio-server" / "clio.log"
  }

  /** Task to clear out the IT log dir before running tests. */
  lazy val resetLogs: Initialize[Task[File]] = Def.task {
    val logDir = logTarget.value
    IO.delete(logDir)
    IO.createDirectory(logDir)
    /*
     * Our Dockerized integration tests tail the clio log to check
     * for the "started" message before starting tests, so we ensure
     * it exists here.
     */
    val clioLog = clioLogFile.value
    IO.touch(clioLog)
    clioLog
  }

  /** Task to get a bearer token for use in integration tests. */
  lazy val getBearerToken: Initialize[Task[String]] = Def.task {
    if (sys.env.get("JENKINS_URL").isEmpty) {
      // If we're not running in Jenkins, we can just shell out to gcloud
      "gcloud auth print-access-token".!!.stripLineEnd
    } else {
      // Otherwise, we have to go through Vault to get a service account's
      // info, through which we can get an access token
      val vaultDriver = {
        val vaultToken = sys.env
          .get("VAULT_TOKEN")
          .orElse(vaultTokenFiles.find(_.exists).map(IO.read(_)))
          .getOrElse {
            sys.error(
              "Vault token not given or found on filesystem, can't get bearer token!"
            )
          }

        val vaultConfig = new VaultConfig()
          .address(vaultUrl)
          .token(vaultToken)
          .build()

        new Vault(vaultConfig)
      }

      val credential = {
        val accountInfo = vaultDriver.logical().read(vaultPath).getData

        ServiceAccountCredentials.fromPkcs8(
          accountInfo.get("client_id"),
          accountInfo.get("client_email"),
          accountInfo.get("private_key"),
          accountInfo.get("private_key_id"),
          authScopes.asJava,
          null,
          new URI(accountInfo.get("token_uri"))
        )
      }

      val token = credential.refreshAccessToken()
      token.getTokenValue
    }
  }

  /** Environment variables to inject when running integration tests. */
  lazy val itEnvVars: Initialize[Task[Map[String, String]]] = Def.task {
    val logDir = logTarget.value
    val clioLog = clioLogFile.value
    val confDir = (classDirectory in IntegrationTest).value / "org" / "broadinstitute" / "clio" / "integrationtest"

    Map(
      "CLIO_DOCKER_TAG" -> version.value,
      "ELASTICSEARCH_DOCKER_TAG" -> Dependencies.ElasticsearchVersion,
      "LOG_DIR" -> logDir.getAbsolutePath,
      "CLIO_LOG_FILE" -> clioLog.getAbsolutePath,
      "CONF_DIR" -> confDir.getAbsolutePath,
      "BEARER_TOKEN" -> getBearerToken.value
    )
  }

  /** Settings to add to the project */
  lazy val settings: Seq[Setting[_]] = {

    Seq.concat(
      Defaults.itSettings,
      inConfig(IntegrationTest) {
        ScalafmtCorePlugin.autoImport.scalafmtSettings ++ Seq(
          scalacOptions in doc ++= Compilation.DocSettings,
          scalacOptions in console := Compilation.ConsoleSettings,
          resourceGenerators += Versioning.writeVersionConfig.taskValue
        )
      },
      Seq(
        /*
         * Override the top-level `test` definition to be a no-op so running "sbt test"
         * won't trigger integration tests along with the unit tests in other projects.
         *
         * Integration tests should be explicitly run with the keys defined below,
         * or "it:test" / "it:testOnly XYZ".
         */
        test := {},
        /*
         * Testcontainers registers a JVM shutdown hook to remove the containers
         * it creates using docker-compose. Forking when running integration tests
         * makes all containers be removed as soon as tests finish running.
         *
         * Forking also allows us to set environment variables for substitution
         * in our docker-compose files.
         */
        fork in IntegrationTest := true,
        envVars in IntegrationTest ++= itEnvVars.value,
        resourceGenerators in IntegrationTest +=
          resetLogs.map(Seq(_)).taskValue
      )
    )
  }
}
