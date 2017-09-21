package org.broadinstitute.clio.server.webservice

import org.broadinstitute.clio.server.service.{
  PersistenceService,
  SearchService,
  WgsUbamService
}
import org.broadinstitute.clio.server.{ClioApp, MockClioApp}

import akka.stream.Materializer

import scala.concurrent.ExecutionContext

class MockWgsUbamWebService(
  app: ClioApp = MockClioApp()
)(implicit executionContext: ExecutionContext, mat: Materializer)
    extends WgsUbamWebService {
  private val persistence = PersistenceService(app)
  private val search = SearchService(app)
  override lazy val wgsUbamService: WgsUbamService =
    new WgsUbamService(persistence, search)
}
