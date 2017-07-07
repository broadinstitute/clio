package org.broadinstitute.clio.server.webservice

import org.broadinstitute.clio.server.service.ReadGroupService
import org.broadinstitute.clio.server.{ClioApp, MockClioApp}

import scala.concurrent.ExecutionContext

class MockReadGroupWebService(
  app: ClioApp = MockClioApp()
)(implicit executionContext: ExecutionContext)
    extends ReadGroupWebService {
  override lazy val readGroupService: ReadGroupService = ReadGroupService(app)
}
