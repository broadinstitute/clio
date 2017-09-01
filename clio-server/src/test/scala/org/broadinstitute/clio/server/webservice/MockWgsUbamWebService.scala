package org.broadinstitute.clio.server.webservice

import org.broadinstitute.clio.server.service.{SearchService, WgsUbamService}
import org.broadinstitute.clio.server.{ClioApp, MockClioApp}

import scala.concurrent.ExecutionContext

class MockWgsUbamWebService(
  app: ClioApp = MockClioApp()
)(implicit executionContext: ExecutionContext)
    extends WgsUbamWebService {
  private val search = SearchService(app)
  override lazy val wgsUbamService: WgsUbamService =
    new WgsUbamService(search)
}
