package org.broadinstitute.clio.util.auth

import org.broadinstitute.clio.util.model.Env

import com.bettercloud.vault.{Vault, VaultConfig}
import com.google.auth.oauth2.ServiceAccountCredentials

import scala.collection.JavaConverters._

import java.io.File
import java.net.URI
import java.nio.file.Files

/**
  * Utility for loading Clio-related secrets from Vault
  * @param env the environment for which secrets should be loaded
  */
class VaultUtil(env: Env) {
  private val vaultDriver: Vault = {
    val config = new VaultConfig()
      .address(VaultUtil.vaultUrl)
      .token(VaultUtil.token)
      .build()

    new Vault(config)
  }

  def credentialForScopes(scopes: Seq[String]): ServiceAccountCredentials = {
    val accountJSONPath =
      s"${VaultUtil.vaultClioPrefix}/${env.entryName}/clio-account.json"
    val accountInfo = vaultDriver.logical().read(accountJSONPath).getData

    ServiceAccountCredentials.fromPkcs8(
      accountInfo.get("client_id"),
      accountInfo.get("client_email"),
      accountInfo.get("private_key"),
      accountInfo.get("private_key_id"),
      scopes.asJava,
      null,
      new URI(accountInfo.get("token_uri"))
    )
  }
}

object VaultUtil {

  /** URL of vault server to use when getting bearer tokens for service accounts. */
  private val vaultUrl = "https://clotho.broadinstitute.org:8200/"

  /** Prefix in Vault for all Clio secrets. */
  private val vaultClioPrefix = "secret/dsde/gotc/clio"

  /** List of possible token-file locations, in order of preference. */
  private val vaultTokenFiles = Seq(
    new File("/etc/vault-token-dsde"),
    new File(s"${System.getProperty("user.home")}/.vault-token")
  )

  def token: String =
    sys.env
      .get("VAULT_TOKEN")
      .orElse {
        VaultUtil.vaultTokenFiles
          .find(_.exists)
          .map { file =>
            new String(Files.readAllBytes(file.toPath)).stripLineEnd
          }
      }
      .getOrElse {
        sys.error("Vault token not given or found on filesystem!")
      }
}
