package org.broadinstitute.clio.integrationtest

import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.util.auth.VaultUtil

import akka.http.scaladsl.model.Uri

/**
  * An integration spec that runs entirely against a Clio instance
  * and elasticsearch cluster deployed into one of our environments.
  *
  * @param env the environment to test against, either "dev", "staging", or "prod"
  */
abstract class EnvIntegrationSpec(env: String)
    extends BaseIntegrationSpec(s"Clio in $env")
    with IntegrationSuite {

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
  override lazy val bearerToken: String = {
    val vaultUtil = new VaultUtil(Env.Test)
    val credential = vaultUtil.credentialForScopes(authScopes)
    credential.refreshAccessToken().getTokenValue
  }
}

/** The integration spec that runs against Clio in dev. */
class DevEnvIntegrationSpec extends EnvIntegrationSpec("dev")

/** The integration spec that runs against Clio in staging. */
class StagingEnvIntegrationSpec extends EnvIntegrationSpec("staging")
