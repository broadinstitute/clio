package org.broadinstitute.clio.integrationtest

import org.broadinstitute.clio.client.webclient.ClioWebClient

import akka.http.scaladsl.model.Uri
import com.bettercloud.vault.{Vault, VaultConfig}
import com.google.auth.oauth2.ServiceAccountCredentials

import java.io.File
import java.net.URI
import java.nio.file.Files
import scala.collection.JavaConverters._
import scala.sys.process._

/**
  * An integration spec that runs entirely against a Clio instance
  * and elasticsearch cluster deployed into one of our environments.
  *
  * @param env the environment to test against, either "dev", "staging", or "prod"
  */
abstract class EnvIntegrationSpec(env: String)
    extends BaseIntegrationSpec(s"Clio in $env")
    with IntegrationSuite {

  /** URL of vault server to use when getting bearer tokens for service accounts. */
  private val vaultUrl = "https://clotho.broadinstitute.org:8200/"

  /**
    * Path within Vault to the service account info Jenkins should use when talking to Clio.
    */
  private val vaultPath = "secret/dsde/gotc/dev/clio/clio-test.json"

  /** List of possible token-file locations, in order of preference. */
  private val vaultTokenFiles = Seq(
    new File("/etc/vault-token-dsde"),
    new File(s"${System.getProperty("user.home")}/.vault-token")
  )

  /** Scopes needed from Google to get past Clio's auth proxy. */
  private val authScopes = Seq(
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/userinfo.email"
  )

  override val clioWebClient: ClioWebClient = new ClioWebClient(
    s"clio101.gotc-$env.broadinstitute.org",
    443,
    useHttps = true
  )

  override val elasticsearchUri: Uri = Uri(
    s"http://elasticsearch1.gotc-$env.broadinstitute.org:9200"
  )

  override lazy val bearerToken: String = {
    if (sys.env.get("JENKINS_URL").isEmpty) {
      // If we're not running in Jenkins, we can just shell out to gcloud
      "gcloud auth print-access-token".!!.stripLineEnd
    } else {
      // Otherwise, we have to go through Vault to get a service account's
      // info, through which we can get an access token
      val vaultToken = sys.env
        .get("VAULT_TOKEN")
        .orElse {
          vaultTokenFiles
            .find(_.exists)
            .map { file =>
              new String(Files.readAllBytes(file.toPath)).stripLineEnd
            }
        }
        .getOrElse {
          sys.error(
            "Vault token not given or found on filesystem, can't get bearer token!"
          )
        }

      val vaultConfig = new VaultConfig()
        .address(vaultUrl)
        .token(vaultToken)
        .build()

      val vaultDriver = new Vault(vaultConfig)
      val accountInfo = vaultDriver.logical().read(vaultPath).getData

      val credential =
        ServiceAccountCredentials.fromPkcs8(
          accountInfo.get("client_id"),
          accountInfo.get("client_email"),
          accountInfo.get("private_key"),
          accountInfo.get("private_key_id"),
          authScopes.asJava,
          null,
          new URI(accountInfo.get("token_uri"))
        )

      credential.refreshAccessToken().getTokenValue
    }
  }
}

/** The integration spec that runs against Clio in dev. */
class DevEnvIntegrationSpec extends EnvIntegrationSpec("dev")

/** The integration spec that runs against Clio in staging. */
class StagingEnvIntegrationSpec extends EnvIntegrationSpec("staging")
