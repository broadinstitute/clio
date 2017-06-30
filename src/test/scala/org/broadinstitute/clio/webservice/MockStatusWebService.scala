package org.broadinstitute.clio.webservice

import org.broadinstitute.clio.service.StatusService
import org.broadinstitute.clio.{ClioApp, MockClioApp}

import scala.concurrent.ExecutionContext

class MockStatusWebService(
  app: ClioApp = MockClioApp()
)(implicit ec: ExecutionContext)
    extends StatusWebService {
  override lazy val statusService: StatusService = StatusService(app)
}
