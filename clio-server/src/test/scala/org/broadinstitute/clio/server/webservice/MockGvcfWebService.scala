package org.broadinstitute.clio.server.webservice

import org.broadinstitute.clio.server.service.{
  PersistenceService,
  SearchService,
  GvcfService
}
import org.broadinstitute.clio.server.{ClioApp, MockClioApp}

import scala.concurrent.ExecutionContext

class MockGvcfWebService(
  app: ClioApp = MockClioApp()
)(implicit executionContext: ExecutionContext)
    extends GvcfWebService {
  private val persistence = PersistenceService(app)
  private val search = SearchService(app)
  override lazy val gvcfService: GvcfService =
    new GvcfService(persistence, search)
}
