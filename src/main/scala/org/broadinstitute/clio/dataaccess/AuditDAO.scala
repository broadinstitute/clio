package org.broadinstitute.clio.dataaccess

import org.broadinstitute.clio.model.{ClioRequest, ClioResponse}

import scala.concurrent.Future

trait AuditDAO {
  def auditRequest(request: ClioRequest): Future[Unit]

  def auditResponse(request: ClioRequest, response: ClioResponse): Future[Unit]

  def auditException(request: ClioRequest, exception: Exception): Future[Unit]
}
