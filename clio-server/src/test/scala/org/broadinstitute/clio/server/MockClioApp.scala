package org.broadinstitute.clio

import org.broadinstitute.clio.dataaccess._

object MockClioApp {
  def apply(
    serverStatusDAO: ServerStatusDAO = new MockServerStatusDAO(),
    auditDAO: AuditDAO = new MockAuditDAO(),
    httpServerDAO: HttpServerDAO = new MockHttpServerDAO(),
    elasticsearchDAO: ElasticsearchDAO = new MockElasticsearchDAO()
  ): ClioApp = {
    new ClioApp(serverStatusDAO, auditDAO, httpServerDAO, elasticsearchDAO)
  }
}
