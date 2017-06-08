package org.broadinstitute.clio

import org.broadinstitute.clio.dataaccess.{AuditDAO, ElasticsearchDAO, HttpServerDAO, ServerStatusDAO}

class ClioApp
(
  val serverStatusDAO: ServerStatusDAO,
  val auditDAO: AuditDAO,
  val httpServerDAO: HttpServerDAO,
  val elasticsearchDAO: ElasticsearchDAO
)
