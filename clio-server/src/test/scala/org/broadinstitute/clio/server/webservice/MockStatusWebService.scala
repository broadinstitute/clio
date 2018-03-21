package org.broadinstitute.clio.server.webservice

import org.broadinstitute.clio.server.service.MockStatusService

import scala.concurrent.ExecutionContext

class MockStatusWebService()(
  implicit executionContext: ExecutionContext
) extends StatusWebService(
      new MockStatusService()
    )
