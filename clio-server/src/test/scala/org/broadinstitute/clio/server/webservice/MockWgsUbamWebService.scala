package org.broadinstitute.clio.server.webservice

import org.broadinstitute.clio.server.service.WgsUbamService
import org.broadinstitute.clio.server.{ClioApp, MockClioApp}

import scala.concurrent.ExecutionContext

class MockWgsUbamWebService(
  app: ClioApp = MockClioApp()
)(implicit executionContext: ExecutionContext)
    extends WgsUbamWebService {
  override lazy val wgsUbamService: WgsUbamService = WgsUbamService(app)
}
