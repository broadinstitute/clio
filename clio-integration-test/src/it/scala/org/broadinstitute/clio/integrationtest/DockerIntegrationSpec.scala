package org.broadinstitute.clio.integrationtest

import org.broadinstitute.clio.client.webclient.ClioWebClient
import akka.NotUsed
import akka.http.scaladsl.model.Uri
import akka.stream.alpakka.file.scaladsl.FileTailSource
import akka.stream.scaladsl.{Sink, Source}
import com.dimafeng.testcontainers.ForAllTestContainer

import scala.concurrent.Await
import scala.concurrent.duration._
import java.io.File
import java.nio.file.FileSystems

import akka.http.scaladsl.model.headers.OAuth2BearerToken

/**
  * An integration spec that spins up a Clio server instance and
  * Elasticsearch cluster in Docker locally using docker-compose,
  * and tests against them.
  *
  * Assumes SBT sets all environment variables required by the
  * docker-compose file when forking to run integration tests.
  * @see `ClioIntegrationTestSettings` in the build
  */
class DockerIntegrationSpec
    extends BaseIntegrationSpec("Clio in Docker")
    with ForAllTestContainer
    with IntegrationSuite {

  // Docker-compose appends "_<instance #>" to service names.
  private val clioFullName = s"${DockerIntegrationSpec.clioServiceName}_1"
  private val esFullName =
    s"${DockerIntegrationSpec.elasticsearchServiceName}_1"

  override val container = new ClioDockerComposeContainer(
    new File(getClass.getResource(DockerIntegrationSpec.composeFilename).toURI),
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
    new ClioWebClient(
      container.getServiceHost(clioFullName),
      container.getServicePort(clioFullName),
      useHttps = false
    )
  override lazy val elasticsearchUri: Uri = container.getServiceUri(esFullName)

  // No bearer token needed for talking to local Clio.
  override implicit val bearerToken: OAuth2BearerToken = OAuth2BearerToken("dummy-token")

  override def beforeAll(): Unit = {
    super.beforeAll()

    /*
     * Testcontainers doesn't provide a way to wait on a docker-compose
     * container to reach a ready state, so we roll our own here.
     */
    val fs = FileSystems.getDefault
    val clioLogLines: Source[String, NotUsed] = FileTailSource.lines(
      path = fs.getPath(sys.env("CLIO_LOG_FILE")),
      maxLineSize = 8192,
      pollingInterval = 250.millis
    )

    val waitTime = 90.seconds
    logger.info(
      s"Waiting up to ${waitTime.toString} for Clio to log startup message..."
    )
    val logStream = clioLogLines
      .map { line =>
        if (!line.contains("DEBUG")) println(line)
        line
      }
      .takeWhile(line => !line.contains(DockerIntegrationSpec.clioReadyMessage))
      .runWith(Sink.ignore)
    val _ = Await.result(logStream, waitTime)
    logger.info("Clio ready!")
  }
}

/*
 * Constants for setting up / connecting into the docker-compose environment.
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
