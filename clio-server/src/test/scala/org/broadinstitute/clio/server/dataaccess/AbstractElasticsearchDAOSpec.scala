package org.broadinstitute.clio.server.dataaccess

import org.apache.http.HttpHost
import org.broadinstitute.clio.server.TestKitSuite
import org.scalatest._

/**
  * Runs a suite of tests on an Elasticsearch docker image.
  *
  * Subclasses should start with a spec that calls initialize().
  *
  * @param actorSystemName The name of the actor system.
  */
abstract class AbstractElasticsearchDAOSpec(actorSystemName: String)
    extends TestKitSuite(actorSystemName)
    with ElasticsearchContainer { this: AsyncTestSuite =>

  private lazy val httpHost =
    new HttpHost(elasticsearchContainerIpAddress, elasticsearchPort)
  final lazy val httpElasticsearchDAO = new HttpElasticsearchDAO(Seq(httpHost))

  override protected def afterAll(): Unit = {
    // Not using regular .close within afterAll. The execution context provided by scalatest doesn't seem to run here.
    httpElasticsearchDAO.closeClient()
    super.afterAll()
  }
}
