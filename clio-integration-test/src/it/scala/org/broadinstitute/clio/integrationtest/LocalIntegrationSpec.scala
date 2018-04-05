package org.broadinstitute.clio.integrationtest
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.headers.GenericHttpCredentials
import better.files.File
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.integrationtest.tests._

class LocalIntegrationSpec extends BaseIntegrationSpec(s"Clio local") {

  /**
    * The web client to use within the tested clio-client.
    */
  override def clioWebClient: ClioWebClient = new ClioWebClient(
    s"localhost",
    8080,
    useHttps = false,
    maxQueuedRequests,
    maxConcurrentRequests,
    clientTimeout,
    maxRequestRetries,
    () => {
      GenericHttpCredentials("fake", "fake")
    }
  )

  /**
    * The URI of the Elasticsearch instance to test in a suite.
    * Could point to a local Docker container, or to a deployed ES node.
    */
  override def elasticsearchUri: Uri = s"http://localhost:9200"

  /**
    * Path to the root directory in which metadata updates will
    * be persisted.
    */
  override def rootPersistenceDir: File = {
    File(ClioBuildInfo.persistenceDir)
  }
}

class LocalBasicSpec extends LocalIntegrationSpec with BasicTests
class LocalUbamSpec extends LocalIntegrationSpec with UbamTests
class LocalCramSpec extends LocalIntegrationSpec with WgsCramTests
class LocalGvcfSpec extends LocalIntegrationSpec with GvcfTests
class LocalArraysSpec extends LocalIntegrationSpec with ArraysTests
