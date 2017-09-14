package org.broadinstitute.clio.integrationtest

import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.util.AuthUtil

import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.headers.OAuth2BearerToken

import java.nio.file.Path

/**
  * An integration spec that runs entirely against a Clio instance
  * and elasticsearch cluster deployed into one of our environments.
  *
  * @param env the environment to test against, either "dev", "staging", or "prod"
  */
abstract class EnvIntegrationSpec(env: String)
    extends BaseIntegrationSpec(s"Clio in $env")
    with IntegrationSuite {

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

  override lazy implicit val bearerToken: OAuth2BearerToken =
    OAuth2BearerToken(
      AuthUtil
        .getCredsFromServiceAccount(serviceAccount)
        .getTokenValue
    )

  override lazy val rootPersistenceDir: Path =
    rootPathForBucketInEnv(env, storageScopes)
}

/** The integration spec that runs against Clio in dev. */
class DevEnvIntegrationSpec extends EnvIntegrationSpec("dev")

/** The integration spec that runs against Clio in staging. */
class StagingEnvIntegrationSpec extends EnvIntegrationSpec("staging")
