package org.broadinstitute.clio.integrationtest

import org.broadinstitute.clio.client.webclient.ClioWebClient

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

  override val clioWebClient: ClioWebClient = new ClioWebClient(
    s"clio101.gotc-$env.broadinstitute.org",
    443,
    useHttps = true
  )

  override val elasticsearchUri: Uri = Uri(
    s"http://elasticsearch1.gotc-$env.broadinstitute.org:9200"
  )
}

/** The integration spec that runs against Clio in dev. */
class DevEnvIntegrationSpec extends EnvIntegrationSpec("dev")

/** The integration spec that runs against Clio in staging. */
class StagingEnvIntegrationSpec extends EnvIntegrationSpec("staging")
