package org.broadinstitute.clio.server

import org.broadinstitute.clio.server.dataaccess.{
  AuditDAO,
  HttpServerDAO,
  SearchDAO,
  ServerStatusDAO
}

class ClioApp(val serverStatusDAO: ServerStatusDAO,
              val auditDAO: AuditDAO,
              val httpServerDAO: HttpServerDAO,
              val searchDAO: SearchDAO)
