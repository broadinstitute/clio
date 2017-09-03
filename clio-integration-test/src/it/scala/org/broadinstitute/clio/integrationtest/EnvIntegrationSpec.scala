package org.broadinstitute.clio.integrationtest

import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.util.model.ServiceAccount

import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import io.circe.syntax._
import com.bettercloud.vault.{Vault, VaultConfig}
import com.google.cloud.storage.StorageOptions
import com.google.cloud.storage.contrib.nio.{
  CloudStorageConfiguration,
  CloudStorageFileSystem
}

import scala.collection.JavaConverters._

import java.io.File
import java.nio.file.{FileSystem, Files, Path}

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

  /** Scopes needed from Google to read Clio's persistence buckets. */
  private val storageScopes = Seq(
    "https://www.googleapis.com/auth/devstorage.read_only"
  )

  override val clioWebClient: ClioWebClient = new ClioWebClient(
    s"clio101.gotc-$env.broadinstitute.org",
    443,
    useHttps = true,
    clientTimeout
  )

  override val elasticsearchUri: Uri = Uri(
    s"http://elasticsearch1.gotc-$env.broadinstitute.org:9200"
  )

  /*
   * We use the Google credential from the Test environment to:
   *   1. Talk through Clio's OpenIDC proxy, and
   *   2. Read the contents of Clio's persistence buckets
   */
  private lazy val serviceAccount: ServiceAccount = {
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

    accountJSON
      .as[ServiceAccount]
      .fold({ err =>
        throw new RuntimeException(
          s"Failed to decode service account JSON from Vault at $vaultPath",
          err
        )
      }, identity)
  }

  override lazy implicit val bearerToken: OAuth2BearerToken = {
    val credential = serviceAccount.credentialForScopes(authScopes)
    OAuth2BearerToken(credential.refreshAccessToken().getTokenValue)
  }

  override lazy val rootPersistenceDir: Path = {
    val storageOptions = StorageOptions
      .newBuilder()
      .setProjectId(s"broad-gotc-$env-storage")
      .setCredentials(serviceAccount.credentialForScopes(storageScopes))
      .build()

    val gcs: FileSystem = CloudStorageFileSystem.forBucket(
      s"broad-gotc-$env-clio",
      CloudStorageConfiguration.DEFAULT,
      storageOptions
    )

    gcs.getPath("/")
  }
}

/** The integration spec that runs against Clio in dev. */
class DevEnvIntegrationSpec extends EnvIntegrationSpec("dev")

/** The integration spec that runs against Clio in staging. */
class StagingEnvIntegrationSpec extends EnvIntegrationSpec("staging")
