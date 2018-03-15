package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.dataaccess.AuditDAO
import org.broadinstitute.clio.server.model.{ClioRequest, ClioResponse}

import scala.concurrent.Future

class AuditService private (auditDAO: AuditDAO) {

  def auditRequest(request: ClioRequest): Future[Unit] = {
    auditDAO.auditRequest(request)
  }

  def auditResponse(request: ClioRequest, response: ClioResponse): Future[Unit] = {
    auditDAO.auditResponse(request, response)
  }

  def auditException(request: ClioRequest, exception: Exception): Future[Unit] = {
    auditDAO.auditException(request, exception)
  }
}

object AuditService {

  def apply(auditDAO: AuditDAO): AuditService = {
    new AuditService(auditDAO)
  }
}
