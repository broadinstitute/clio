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

/**
  * An integration spec that spins up a Clio server instance
  * in Docker locally using docker-compose, and tests against it.
  *
  * Assumes SBT sets all environment variables required by the
  * docker-compose file when forking to run integration tests.
  *
  * @param composeFile the compose file describing how to spin up Clio
  * @param esDescription description of Elasticsearch to use in the spec name
  * @see `ClioIntegrationTestSettings` in the build
  */
abstract class DockerIntegrationSpec(composeFile: String, esDescription: String)
    extends BaseIntegrationSpec(
      s"Containerized Clio - connecting to $esDescription Elasticsearch"
    )
    with ForAllTestContainer
    with IntegrationSuite {

  /**
    * The hostname of the Elasticsearch instance to which the Dockerized Clio should connect.
    */
  def elasticsearchHostname: String

  /**
    * Name of the clio-server instance that will be started by the docker-compose
    * call, plus its instance number.
    *
    * NOTE: If you change the name of the Clio service in the compose files used
    * for integration testing, you also need to change this value to match.
    */
  private val clioName = "clio-server_1"
  protected def exposedServices: Map[String, Int] = Map(clioName -> 8080)

  override val container =
    new ClioDockerComposeContainer(
      new File(getClass.getResource(composeFile).toURI),
      elasticsearchHostname,
      exposedServices
    )

  /*
   * Needs to be lazy because `container.getServiceXYZ` will raise an IllegalStateException
   * if it's called before the container is actually started.
   */
  override lazy val clioWebClient: ClioWebClient =
    new ClioWebClient(
      container.getServiceHost(clioName),
      container.getServicePort(clioName),
      useHttps = false
    )

  // No bearer token needed for talking to local Clio.
  override val bearerToken = "dummy-token"

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
      .takeWhile(line => !line.contains("Server started"))
      .runWith(Sink.ignore)
    val _ = Await.result(logStream, waitTime)
    logger.info("Clio ready!")
  }
}

/**
  * The integration spec that tests Dockerized Clio using a local Elasticsearch
  * cluster also running in Docker.
  */
class FullDockerIntegrationSpec
    extends DockerIntegrationSpec("docker-compose.yml", "local") {

  /*
   * Name of one of the elasticsearch instances that will be started by
   * the docker-compose call.
   *
   * NOTE: If you change the name of the elasticserach service in docker-compose.yml,
   * you also need to change this value to match.
   */
  override lazy val elasticsearchHostname: String = "elasticsearch1"

  override def exposedServices: Map[String, Int] =
    super.exposedServices + (s"${elasticsearchHostname}_1" -> 9200)

  override lazy val elasticsearchUri: Uri =
    container.getServiceUri(s"${elasticsearchHostname}_1")
}

/**
  * The integration spec that tests Dockerized Clio using the Elasticsearch
  * cluster deployed in dev .
  */
class DockerDevIntegrationSpec
    extends DockerIntegrationSpec("docker-compose-dev-elasticsearch.yml", "dev") {

  override lazy val elasticsearchHostname =
    "elasticsearch1.gotc-dev.broadinstitute.org"

  override val elasticsearchUri: Uri =
    Uri(s"http://$elasticsearchHostname:9200")
}
