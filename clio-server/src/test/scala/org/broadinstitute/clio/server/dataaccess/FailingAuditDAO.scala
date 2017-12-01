package org.broadinstitute.clio.server.dataaccess

import org.broadinstitute.clio.server.model.{ClioRequest, ClioResponse}

import scala.concurrent.Future

class FailingAuditDAO extends MockAuditDAO {
  override def auditRequest(request: ClioRequest): Future[Unit] = {
    Future.failed(new FailingAuditDAO.AuditRequestFailure)
  }

  override def auditResponse(
    request: ClioRequest,
    response: ClioResponse
  ): Future[Unit] = {
    Future.failed(new FailingAuditDAO.AuditResponseFailure)
  }

  override def auditException(
    request: ClioRequest,
    exception: Exception
  ): Future[Unit] = {
    Future.failed(new FailingAuditDAO.AuditExceptionFailure)
  }
}

object FailingAuditDAO {
  // Define singleton exception instances for the mock to return so the tests that assert
  // failure can match on something more precise than RuntimeException.
  class AuditRequestFailure extends RuntimeException("Expected audit request failure")
  class AuditResponseFailure extends RuntimeException("Expected audit response failure")
  class AuditExceptionFailure extends RuntimeException("Expected audit exception failure")
}
