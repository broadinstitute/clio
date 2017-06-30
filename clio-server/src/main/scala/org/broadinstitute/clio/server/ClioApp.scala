package org.broadinstitute.clio.server

import org.broadinstitute.clio.server.dataaccess.{
  AuditDAO,
  ElasticsearchDAO,
  HttpServerDAO,
  ServerStatusDAO
}

class ClioApp(val serverStatusDAO: ServerStatusDAO,
              val auditDAO: AuditDAO,
              val httpServerDAO: HttpServerDAO,
              val elasticsearchDAO: ElasticsearchDAO)
