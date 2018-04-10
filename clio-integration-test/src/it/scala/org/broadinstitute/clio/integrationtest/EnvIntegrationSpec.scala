package org.broadinstitute.clio.integrationtest

import akka.http.scaladsl.model.Uri
import better.files.File
import com.google.auth.oauth2.OAuth2Credentials
import org.broadinstitute.clio.client.webclient.{
  ClioWebClient,
  GoogleCredentialsGenerator
}
import org.broadinstitute.clio.integrationtest.tests._
import org.broadinstitute.clio.util.auth.AuthUtil

/**
  * An integration spec that runs entirely against a Clio instance
  * and elasticsearch cluster deployed into one of our environments.
  *
  * @param env the environment to test against, either "dev", "staging", or "prod"
  */
abstract class EnvIntegrationSpec(env: String)
    extends BaseIntegrationSpec(s"Clio in $env") {

  /** Scopes needed from Google to read Clio's persistence buckets. */
  private val storageScopes = Seq(
    "https://www.googleapis.com/auth/devstorage.read_only"
  )

  protected lazy val googleCredential: OAuth2Credentials = AuthUtil
    .getCredsFromServiceAccount(serviceAccount)
    .fold(
      fail(s"Couldn't get access token for account $serviceAccount", _),
      identity
    )

  override val clioWebClient: ClioWebClient = ClioWebClient(
    new GoogleCredentialsGenerator(googleCredential),
    s"clio.gotc-$env.broadinstitute.org",
    443,
    useHttps = true
  )

  override val elasticsearchUri: Uri = Uri(
    s"http://elasticsearch1.gotc-$env.broadinstitute.org:9200"
  )

  override lazy val rootPersistenceDir: File =
    rootPathForBucketInEnv(env, storageScopes)
}

/** The integration specs that run against Clio in dev. */
class DevEnvBasicSpec extends EnvIntegrationSpec("dev") with BasicTests
class DevEnvUbamSpec extends EnvIntegrationSpec("dev") with UbamTests
class DevEnvCramSpec extends EnvIntegrationSpec("dev") with WgsCramTests
class DevEnvGvcfSpec extends EnvIntegrationSpec("dev") with GvcfTests
class DevEnvArraysSpec extends EnvIntegrationSpec("dev") with ArraysTests

/**
  * Integration spec checking that auth tokens properly refresh in the client.
  * Only meaningful if run against a deployed Clio with a running proxy.
  */
class AuthIntegrationSpec extends EnvIntegrationSpec("dev") with AuthRefreshTests

/** The integration specs that run against Clio in staging. */
class StagingEnvBasicSpec extends EnvIntegrationSpec("staging") with BasicTests
class StagingEnvUbamSpec extends EnvIntegrationSpec("staging") with UbamTests
class StagingEnvCramSpec extends EnvIntegrationSpec("staging") with WgsCramTests
class StagingEnvGvcfSpec extends EnvIntegrationSpec("staging") with GvcfTests
class StagingEnvArraysSpec extends EnvIntegrationSpec("staging") with ArraysTests
