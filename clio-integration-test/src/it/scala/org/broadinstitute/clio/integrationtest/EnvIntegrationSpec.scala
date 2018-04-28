package org.broadinstitute.clio.integrationtest

import akka.http.scaladsl.model.Uri
import better.files.File
import org.broadinstitute.clio.integrationtest.tests._

/**
  * An integration spec that runs entirely against a Clio instance
  * and elasticsearch cluster deployed into one of our environments.
  *
  * @param env the environment to test against, either "dev", "staging", or "prod"
  */
abstract class EnvIntegrationSpec(env: String)
    extends BaseIntegrationSpec(s"Clio in $env") {

  override val clioHostname = s"clio.gotc-$env.broadinstitute.org"
  override val clioPort = 443
  override val useHttps = true

  override val elasticsearchUri: Uri = Uri(
    s"http://elasticsearch1.gotc-$env.broadinstitute.org:9200"
  )

  override lazy val rootPersistenceDir: File =
    rootPathForBucketInEnv(env, readOnly = true)
}

/** The integration specs that run against Clio in dev. */
class DevEnvBasicSpec extends EnvIntegrationSpec("dev") with BasicTests
class DevEnvUbamSpec extends EnvIntegrationSpec("dev") with UbamTests
class DevEnvCramSpec extends EnvIntegrationSpec("dev") with WgsCramTests
class DevEnvGvcfSpec extends EnvIntegrationSpec("dev") with GvcfTests
class DevEnvArraysSpec extends EnvIntegrationSpec("dev") with ArraysTests

/** The integration specs that run against Clio in staging. */
class StagingEnvBasicSpec extends EnvIntegrationSpec("staging") with BasicTests
class StagingEnvUbamSpec extends EnvIntegrationSpec("staging") with UbamTests
class StagingEnvCramSpec extends EnvIntegrationSpec("staging") with WgsCramTests
class StagingEnvGvcfSpec extends EnvIntegrationSpec("staging") with GvcfTests
class StagingEnvArraysSpec extends EnvIntegrationSpec("staging") with ArraysTests
