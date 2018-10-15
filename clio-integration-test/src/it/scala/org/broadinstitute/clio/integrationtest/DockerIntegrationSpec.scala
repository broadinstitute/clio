package org.broadinstitute.clio.integrationtest

import akka.http.scaladsl.model.Uri
import better.files.File
import com.dimafeng.testcontainers.ForAllTestContainer
import org.broadinstitute.clio.integrationtest.tests._

/**
  * An integration spec that spins up a Clio server instance and
  * Elasticsearch cluster in Docker locally using docker-compose,
  * and tests against them.
  *
  * Assumes SBT sets all environment variables required by the
  * docker-compose file when forking to run integration tests.
  * @see `ClioIntegrationTestSettings` in the build
  */
abstract class DockerIntegrationSpec
    extends BaseIntegrationSpec("Clio in Docker")
    with ForAllTestContainer {

  override val container: ClioDockerComposeContainer =
    ClioDockerComposeContainer.waitForReadyLog(File(IntegrationBuildInfo.tmpDir))

  // SBT sets a local path for persisting metadata updates.
  override lazy val rootPersistenceDir: File = container.persistenceDir

  /*
   * These need to be lazy because the `container.getServiceXYZ` methods will
   * raise an IllegalStateException if they're called before the container is
   * actually started.
   */
  override lazy val clioHostname: String = container.clioHost
  override lazy val clioPort: Int = container.clioPort
  override lazy val elasticsearchUri: Uri =
    s"http://${container.esHost}:${container.esPort}"

  override val useHttps = false
}

/** Dockerized versions of the integration tests that also run against our deployed Clios. */
class CoreDockerBasicSpec extends DockerIntegrationSpec with BasicTests
class CoreDockerUbamSpec extends DockerIntegrationSpec with UbamTests
class CoreDockerCramSpec extends DockerIntegrationSpec with CramTests
class CoreDockerGvcfSpec extends DockerIntegrationSpec with GvcfTests
class CoreDockerArraysSpec extends DockerIntegrationSpec with ArraysTests

/** Load tests. Should only be run against Docker. */
class LoadIntegrationSpec extends DockerIntegrationSpec with LoadTests
