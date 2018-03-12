package org.broadinstitute.clio.server.webservice

import org.broadinstitute.clio.server.dataaccess._
import org.broadinstitute.clio.server.service.StatusService

import scala.concurrent.ExecutionContext

class MockStatusWebService(
  serverStatusDao: ServerStatusDAO = new MockServerStatusDAO(),
  searchDao: SearchDAO = new MockSearchDAO(),
  httpDao: HttpServerDAO = new MockHttpServerDAO()
)(
  implicit executionContext: ExecutionContext
) extends StatusWebService(
      StatusService(
        serverStatusDao,
        searchDao,
        httpDao
      )
    )
