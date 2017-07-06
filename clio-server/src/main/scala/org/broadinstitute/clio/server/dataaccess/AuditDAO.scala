package org.broadinstitute.clio.server.dataaccess

import org.broadinstitute.clio.server.model.{ClioRequest, ClioResponse}

import scala.concurrent.Future

trait AuditDAO {
  def auditRequest(request: ClioRequest): Future[Unit]

  def auditResponse(request: ClioRequest, response: ClioResponse): Future[Unit]

  def auditException(request: ClioRequest, exception: Exception): Future[Unit]
}
