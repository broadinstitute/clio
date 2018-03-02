package org.broadinstitute.clio.server.webservice

import org.broadinstitute.clio.server.service.{
  PersistenceService,
  SearchService,
  WgsCramService
}
import org.broadinstitute.clio.server.{ClioApp, MockClioApp}

import scala.concurrent.ExecutionContext

class MockWgsCramWebService(app: ClioApp = MockClioApp())(
  implicit executionContext: ExecutionContext
) extends WgsCramWebService(
      new WgsCramService(PersistenceService(app), SearchService(app))
    )
    with JsonWebService
