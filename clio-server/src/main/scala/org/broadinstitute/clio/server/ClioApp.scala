package org.broadinstitute.clio.server

import org.broadinstitute.clio.server.dataaccess._

class ClioApp(
  val serverStatusDAO: ServerStatusDAO,
  val auditDAO: AuditDAO,
  val persistenceDAO: PersistenceDAO,
  val searchDAO: SearchDAO
)
