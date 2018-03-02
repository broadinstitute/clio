package org.broadinstitute.clio.server.webservice

import org.broadinstitute.clio.server.service.{
  GvcfService,
  PersistenceService,
  SearchService
}
import org.broadinstitute.clio.server.{ClioApp, MockClioApp}

import scala.concurrent.ExecutionContext

class MockGvcfWebService(app: ClioApp = MockClioApp())(
  implicit executionContext: ExecutionContext
) extends GvcfWebService(new GvcfService(PersistenceService(app), SearchService(app)))
    with JsonWebService
