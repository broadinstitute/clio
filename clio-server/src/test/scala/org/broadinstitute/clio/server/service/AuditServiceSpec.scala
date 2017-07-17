package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.server.MockClioApp
import org.broadinstitute.clio.server.dataaccess.{
  FailingAuditDAO,
  MemoryAuditDAO,
  MockAuditDAO
}
import org.broadinstitute.clio.server.model.{ClioRequest, ClioResponse}
import org.scalatest.{AsyncFlatSpec, Matchers}

class AuditServiceSpec extends AsyncFlatSpec with Matchers {
  behavior of "AuditService"

  it should "auditRequest" in {
    val memoryAuditDAO = new MemoryAuditDAO()
    val app = MockClioApp(auditDAO = memoryAuditDAO)
    val auditService = AuditService(app)
    for {
      _ <- auditService.auditRequest(
        ClioRequest(MockAuditDAO.RequestContentMock)
      )
    } yield {
      val result = ClioRequest(MockAuditDAO.RequestContentMock)
      memoryAuditDAO.auditRequests should be(Seq(result))
      memoryAuditDAO.auditResponses should be(empty)
      memoryAuditDAO.auditExceptions should be(empty)
    }
  }

  it should "auditResponse" in {
    val memoryAuditDAO = new MemoryAuditDAO()
    val app = MockClioApp(auditDAO = memoryAuditDAO)
    val auditService = AuditService(app)
    for {
      _ <- auditService.auditResponse(
        ClioRequest(MockAuditDAO.RequestContentMock),
        ClioResponse(MockAuditDAO.ResponseContentMock)
      )
    } yield {
      val result = (
        ClioRequest(MockAuditDAO.RequestContentMock),
        ClioResponse(MockAuditDAO.ResponseContentMock)
      )
      memoryAuditDAO.auditRequests should be(empty)
      memoryAuditDAO.auditResponses should be(Seq(result))
      memoryAuditDAO.auditExceptions should be(empty)
    }
  }

  it should "auditException" in {
    val memoryAuditDAO = new MemoryAuditDAO()
    val app = MockClioApp(auditDAO = memoryAuditDAO)
    val auditService = AuditService(app)
    for {
      _ <- auditService.auditException(
        ClioRequest(MockAuditDAO.RequestContentMock),
        MockAuditDAO.ExceptionMock
      )
    } yield {
      val result = (
        ClioRequest(MockAuditDAO.RequestContentMock),
        MockAuditDAO.ExceptionMock
      )
      memoryAuditDAO.auditRequests should be(empty)
      memoryAuditDAO.auditResponses should be(empty)
      memoryAuditDAO.auditExceptions should be(Seq(result))
    }
  }

  it should "fail when auditRequest fails" in {
    val failingAuditDAO = new FailingAuditDAO()
    val app = MockClioApp(auditDAO = failingAuditDAO)
    val auditService = AuditService(app)

    recoverToSucceededIf[FailingAuditDAO.AuditRequestFailure] {
      auditService.auditRequest(ClioRequest(MockAuditDAO.RequestContentMock))
    }
  }

  it should "fail when auditResponse fails" in {
    val failingAuditDAO = new FailingAuditDAO()
    val app = MockClioApp(auditDAO = failingAuditDAO)
    val auditService = AuditService(app)

    recoverToSucceededIf[FailingAuditDAO.AuditResponseFailure] {
      auditService.auditResponse(
        ClioRequest(MockAuditDAO.RequestContentMock),
        ClioResponse(MockAuditDAO.ResponseContentMock)
      )
    }
  }

  it should "fail when auditException fails" in {
    val failingAuditDAO = new FailingAuditDAO()
    val app = MockClioApp(auditDAO = failingAuditDAO)
    val auditService = AuditService(app)

    recoverToSucceededIf[FailingAuditDAO.AuditExceptionFailure] {
      auditService.auditException(
        ClioRequest(MockAuditDAO.RequestContentMock),
        MockAuditDAO.ExceptionMock
      )
    }
  }
}
