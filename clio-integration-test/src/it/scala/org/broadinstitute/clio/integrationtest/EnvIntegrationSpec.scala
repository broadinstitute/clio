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
abstract class DevIntegrationSpec extends EnvIntegrationSpec("dev")
class DevEnvBasicSpec extends DevIntegrationSpec with BasicTests
class DevEnvUbamSpec extends DevIntegrationSpec with UbamTests
class DevEnvCramSpec extends DevIntegrationSpec with CramTests
class DevEnvWgsCramSpec extends DevIntegrationSpec with WgsCramTests
class DevEnvGvcfSpec extends DevIntegrationSpec with GvcfTests
class DevEnvArraysSpec extends DevIntegrationSpec with ArraysTests

/** The integration specs that run against Clio in staging. */
abstract class StagingIntegrationSpec extends EnvIntegrationSpec("staging")
class StagingEnvBasicSpec extends StagingIntegrationSpec with BasicTests
class StagingEnvUbamSpec extends StagingIntegrationSpec with UbamTests
class StagingEnvCramSpec extends StagingIntegrationSpec with CramTests
class StagingEnvWgsCramSpec extends StagingIntegrationSpec with WgsCramTests
class StagingEnvGvcfSpec extends StagingIntegrationSpec with GvcfTests
class StagingEnvArraysSpec extends StagingIntegrationSpec with ArraysTests
