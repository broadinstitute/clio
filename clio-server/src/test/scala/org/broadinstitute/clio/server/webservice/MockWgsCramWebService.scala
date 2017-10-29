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
) extends WgsCramWebService
    with JsonWebService {
  private val persistence = PersistenceService(app)
  private val search = SearchService(app)
  override lazy val wgsCramService: WgsCramService =
    new WgsCramService(persistence, search)
}
