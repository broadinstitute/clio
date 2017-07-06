package org.broadinstitute.clio.server.dataaccess

import org.broadinstitute.clio.server.model.{ClioRequest, ClioResponse}

import scala.concurrent.Future

class MockAuditDAO extends AuditDAO {
  override def auditRequest(request: ClioRequest): Future[Unit] =
    Future.successful(())

  override def auditResponse(request: ClioRequest,
                             response: ClioResponse): Future[Unit] =
    Future.successful(())

  override def auditException(request: ClioRequest,
                              exception: Exception): Future[Unit] =
    Future.successful(())
}

object MockAuditDAO {
  val RequestContentMock = "Request"
  val ResponseContentMock = "Response"
  val ExceptionMock = new RuntimeException("Expected")
}
