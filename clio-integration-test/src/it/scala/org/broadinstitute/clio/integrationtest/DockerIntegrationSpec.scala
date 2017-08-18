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

  private val clioName = "clio-server_1"
  protected def exposedServices: Map[String, Int] = Map(clioName -> 8080)

  override val container =
    new ClioDockerComposeContainer(
      new File(getClass.getResource(composeFile).toURI),
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

  override def beforeAll(): Unit = {
    super.beforeAll()

    /*
     * Testcontainers doesn't provide a way to wait on a docker-compose
     * container to reach a ready state, so we roll our own here.
     */
    val fs = FileSystems.getDefault
    val clioLogLines: Source[String, NotUsed] = FileTailSource.lines(
      path = fs.getPath(System.getenv("LOG_DIR"), "clio-server", "clio.log"),
      maxLineSize = 8192,
      pollingInterval = 250.millis
    )

    val waitTime = 90.seconds
    logger.info(
      s"Waiting up to ${waitTime.toString} for Clio to log startup message..."
    )
    val logStream = clioLogLines
      .map { line =>
        if (line.contains("INFO")) println(line)
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

  override def exposedServices: Map[String, Int] =
    super.exposedServices + ("elasticsearch1_1" -> 9200)

  override lazy val elasticsearchUri: Uri =
    container.getServiceUri("elasticsearch1_1")
}

/**
  * The integration spec taht tests Dockerized Clio using the Elasticsearch
  * cluster deployed in dev.
  */
class DockerDevIntegrationSpec
    extends DockerIntegrationSpec("docker-compose-dev-elasticsearch.yml", "dev") {
  override val elasticsearchUri: Uri =
    Uri("http://elasticsearch1.gotc-dev.broadinstitute.org:9200")
}
