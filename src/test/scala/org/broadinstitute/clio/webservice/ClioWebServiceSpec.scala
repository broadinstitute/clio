package org.broadinstitute.clio.webservice

import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.broadinstitute.clio.ClioApp
import org.broadinstitute.clio.dataaccess.MockHttpServerDAO
import org.broadinstitute.clio.service.StatusService
import org.scalatest.Suite

/**
  * Mixin for testing a clio webservice.
  */
trait ClioWebServiceSpec extends ClioWebService with ScalatestRouteTest {
  this: Suite =>

  lazy val httpServerDAO = new MockHttpServerDAO()

  lazy val app = new ClioApp(httpServerDAO)

  lazy val statusService = StatusService(app)
}
