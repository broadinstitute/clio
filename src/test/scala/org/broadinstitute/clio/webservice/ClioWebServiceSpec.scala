package org.broadinstitute.clio.webservice

import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.broadinstitute.clio.MockClioApp
import org.broadinstitute.clio.service.StatusService
import org.scalatest.Suite

/**
  * Mixin for testing a clio webservice.
  */
trait ClioWebServiceSpec extends ClioWebService with ScalatestRouteTest {
  this: Suite =>

  lazy val app = MockClioApp()

  lazy val statusService = StatusService(app)
}
