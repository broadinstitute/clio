package org.broadinstitute.clio.webservice

import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.broadinstitute.clio.ClioApp
import org.broadinstitute.clio.dataaccess.{MockElasticsearchDAO, MockHttpServerDAO, MockServerStatusDAO}
import org.broadinstitute.clio.service.StatusService
import org.scalatest.Suite

/**
  * Mixin for testing a clio webservice.
  */
trait ClioWebServiceSpec extends ClioWebService with ScalatestRouteTest {
  this: Suite =>

  lazy val serverStatusDAO = new MockServerStatusDAO()
  lazy val httpServerDAO = new MockHttpServerDAO()
  lazy val elasticsearchDAO = new MockElasticsearchDAO()

  lazy val app = new ClioApp(serverStatusDAO, httpServerDAO, elasticsearchDAO)

  lazy val statusService = StatusService(app)
}
