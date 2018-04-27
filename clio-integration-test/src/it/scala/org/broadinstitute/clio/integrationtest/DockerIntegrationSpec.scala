package org.broadinstitute.clio.integrationtest

import akka.NotUsed
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.alpakka.file.scaladsl.FileTailSource
import akka.stream.scaladsl.{Sink, Source}
import better.files.File
import com.dimafeng.testcontainers.ForAllTestContainer
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.integrationtest.tests._

import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * An integration spec that spins up a Clio server instance and
  * Elasticsearch cluster in Docker locally using docker-compose,
  * and tests against them.
  *
  * Assumes SBT sets all environment variables required by the
  * docker-compose file when forking to run integration tests.
  * @see `ClioIntegrationTestSettings` in the build
  */
abstract class DockerIntegrationSpec(
  name: String,
  startupMessage: String = DockerIntegrationSpec.clioReadyMessage
) extends BaseIntegrationSpec(name)
    with ForAllTestContainer {

  // Docker-compose appends "_<instance #>" to service names.
  protected val clioFullName = s"${DockerIntegrationSpec.clioServiceName}_1"
  protected val esFullName =
    s"${DockerIntegrationSpec.elasticsearchServiceName}_1"

  override val container = new ClioDockerComposeContainer(
    File(getClass.getResource(DockerIntegrationSpec.composeFilename)),
    DockerIntegrationSpec.elasticsearchServiceName,
    Map(
      clioFullName -> DockerIntegrationSpec.clioServicePort,
      esFullName -> DockerIntegrationSpec.elasticsearchServicePort
    )
  )

  /*
   * These need to be lazy because the `container.getServiceXYZ` methods will
   * raise an IllegalStateException if they're called before the container is
   * actually started.
   */
  override lazy val clioWebClient: ClioWebClient =
    ClioWebClient(
      () => OAuth2BearerToken("fake-token"),
      container.getServiceHost(clioFullName),
      container.getServicePort(clioFullName),
      useHttps = false
    )
  override lazy val elasticsearchUri: Uri = container.getServiceUri(esFullName)

  // SBT sets a local path for persisting metadata updates.
  override val rootPersistenceDir: File = File(ClioBuildInfo.persistenceDir)

  /*
   * Testcontainers doesn't provide a way to wait on a docker-compose
   * container to reach a ready state, so we roll our own here.
   */
  val clioLogLines: Source[String, NotUsed] = FileTailSource.lines(
    path = ClioDockerComposeContainer.clioLog.path,
    maxLineSize = 1048576,
    pollingInterval = 250.millis
  )

  override def beforeAll(): Unit = {
    super.beforeAll()

    val waitTime = 90.seconds
    logger.info(
      s"Waiting up to ${waitTime.toString} for Clio to log startup message..."
    )
    val logStream = clioLogLines.map { line =>
      if (line.contains("INFO") ||
          line.contains("WARN") ||
          line.contains("ERROR")) {
        println(line)
      }
      line
    }.takeWhile(!_.contains(startupMessage))
      .runWith(Sink.ignore)
    val _ = Await.result(logStream, waitTime)
    logger.info("Clio ready!")
  }
}

/** Dockerized versions of the integration tests that also run against our deployed Clios. */
class CoreDockerBasicSpec extends DockerIntegrationSpec("Clio in Docker") with BasicTests
class CoreDockerUbamSpec extends DockerIntegrationSpec("Clio in Docker") with UbamTests
class CoreDockerCramSpec extends DockerIntegrationSpec("Clio in Docker") with CramTests

class CoreDockerWgsCramSpec
    extends DockerIntegrationSpec("Clio in Docker")
    with WgsCramTests
class CoreDockerGvcfSpec extends DockerIntegrationSpec("Clio in Docker") with GvcfTests

class CoreDockerArraysSpec
    extends DockerIntegrationSpec("Clio in Docker")
    with ArraysTests

/** Load tests. Should only be run against Docker. */
class LoadIntegrationSpec extends DockerIntegrationSpec("Clio under load") with LoadTests

/**
  * Container for constants for setting up / connecting into the docker-compose environment.
  *
  * NOTE: These values depend on:
  *
  *   1. The name / contents of our compose file,
  *   2. The default ports of our Dockerized services,
  *   3. The expected startup messages of clio-server.
  *
  * If you change any of those things, you need to update these constants to match.
  */
object DockerIntegrationSpec {

  /**
    * Filename of the compose file describing the test environment.
    * The file is assumed to be located in the resources directory of the
    * integration-test code, under the same package as this class.
    */
  val composeFilename = "docker-compose.yml"

  /**
    * Service name of the clio-server instance that will be started by
    * the docker-compose call.
    */
  val clioServiceName = "clio-server"

  /** Port we expect the clio-server instance to bind. */
  val clioServicePort = 8080

  /** Message we expect Clio to log when it's ready to accept connections. */
  val clioReadyMessage = "Server started"

  /**
    * Service name of the elasticsearch instance, started by the docker-
    * compose call, to which the clio-server service should connect.
    */
  val elasticsearchServiceName = "elasticsearch1"

  /** Port we expect the elasticsearch instance to bind. */
  val elasticsearchServicePort = 9200
}
