package org.broadinstitute.clio.server.webservice

import org.broadinstitute.clio.server.service.StatusService
import org.broadinstitute.clio.server.{ClioApp, MockClioApp}

import scala.concurrent.ExecutionContext

class MockStatusWebService(
    app: ClioApp = MockClioApp()
)(implicit executionContext: ExecutionContext)
    extends StatusWebService {
  override lazy val statusService: StatusService = StatusService(app)
}
