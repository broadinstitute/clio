package org.broadinstitute.clio.dataaccess

import org.broadinstitute.clio.model.{ClioRequest, ClioResponse}

import scala.concurrent.Future

class MemoryAuditDAO extends MockAuditDAO {
  var auditRequests: Seq[ClioRequest] = Seq.empty
  var auditResponses: Seq[(ClioRequest, ClioResponse)] = Seq.empty
  var auditExceptions: Seq[(ClioRequest, Exception)] = Seq.empty

  override def auditRequest(request: ClioRequest): Future[Unit] = {
    auditRequests :+= request
    super.auditRequest(request)
  }

  override def auditResponse(request: ClioRequest,
                             response: ClioResponse): Future[Unit] = {
    auditResponses :+= ((request, response))
    super.auditResponse(request, response)
  }

  override def auditException(request: ClioRequest,
                              exception: Exception): Future[Unit] = {
    auditExceptions :+= ((request, exception))
    super.auditException(request, exception)
  }
}
