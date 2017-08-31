package org.broadinstitute.clio.integrationtest

import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.util.model.ServiceAccount
import akka.http.scaladsl.model.Uri
import io.circe.syntax._
import com.bettercloud.vault.{Vault, VaultConfig}

import scala.collection.JavaConverters._
import java.io.File
import java.nio.file.Files

import akka.http.scaladsl.model.headers.OAuth2BearerToken

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

  /** Path in vault to the service account JSON to use in testing. */
  private val vaultPath = "secret/dsde/gotc/clio/test/clio-account.json"

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

  /*
   * We use the Google credential from the Test environment
   * to talk through the OpenIDC proxy.
   */
  override lazy implicit val bearerToken: OAuth2BearerToken = {
    val vaultToken: String = vaultTokenFiles
      .find(_.exists)
      .map { file =>
        new String(Files.readAllBytes(file.toPath)).stripLineEnd
      }
      .getOrElse {
        sys.error("Vault token not found on filesystem!")
      }

    val vaultConfig = new VaultConfig()
      .address(vaultUrl)
      .token(vaultToken)
      .build()

    val vaultDriver = new Vault(vaultConfig)
    val accountJSON =
      vaultDriver
        .logical()
        .read(vaultPath)
        .getData
        .asScala
        .toMap[String, String]
        .asJson

    val serviceAccount: ServiceAccount =
      accountJSON
        .as[ServiceAccount]
        .fold({ err =>
          throw new RuntimeException(
            s"Failed to decode service account JSON from Vault at $vaultPath",
            err
          )
        }, identity)

    val credential = serviceAccount.credentialForScopes(authScopes)
    OAuth2BearerToken(credential.refreshAccessToken().getTokenValue)
  }
}

/** The integration spec that runs against Clio in dev. */
class DevEnvIntegrationSpec extends EnvIntegrationSpec("dev")

/** The integration spec that runs against Clio in staging. */
class StagingEnvIntegrationSpec extends EnvIntegrationSpec("staging")
