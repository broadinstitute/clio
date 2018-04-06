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

class LocalIntegrationSpec extends BaseIntegrationSpec(s"Clio local") {

  protected lazy val googleCredential: OAuth2Credentials = AuthUtil
    .getCredsFromServiceAccount(serviceAccount)
    .fold(
      fail(s"Couldn't get access token for account $serviceAccount", _),
      identity
    )

  /**
    * The web client to use within the tested clio-client.
    */
  override def clioWebClient: ClioWebClient = new ClioWebClient(
    s"localhost",
    8080,
    useHttps = false,
    clientTimeout,
    maxRequestRetries,
    new GoogleCredentialsGenerator(googleCredential)
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
