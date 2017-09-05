package org.broadinstitute.clio.server

import org.broadinstitute.clio.server.dataaccess._

object MockClioApp {
  def apply(serverStatusDAO: ServerStatusDAO = new MockServerStatusDAO(),
            auditDAO: AuditDAO = new MockAuditDAO(),
            httpServerDAO: HttpServerDAO = new MockHttpServerDAO(),
            persistenceDAO: PersistenceDAO = new MockPersistenceDAO(),
            searchDAO: SearchDAO = new MockSearchDAO()): ClioApp = {
    new ClioApp(
      serverStatusDAO,
      auditDAO,
      httpServerDAO,
      persistenceDAO,
      searchDAO
    )
  }
}
