package org.broadinstitute.clio.server.webservice

import org.broadinstitute.clio.server.service.{
  PersistenceService,
  SearchService,
  WgsUbamService
}
import org.broadinstitute.clio.server.{ClioApp, MockClioApp}

import scala.concurrent.ExecutionContext

class MockWgsUbamWebService(app: ClioApp = MockClioApp())(
  implicit executionContext: ExecutionContext
) extends WgsUbamWebService(
      new WgsUbamService(PersistenceService(app), SearchService(app))
    )
    with JsonWebService
