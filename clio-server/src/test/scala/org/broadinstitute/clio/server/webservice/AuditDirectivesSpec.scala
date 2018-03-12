package org.broadinstitute.clio.server.webservice

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.ExceptionHandler
import org.broadinstitute.clio.server.dataaccess.{FailingAuditDAO, MockAuditDAO}
import org.broadinstitute.clio.server.model.MockResult

class AuditDirectivesSpec extends BaseWebserviceSpec {
  behavior of "AuditDirectives"

  it should "auditRequest" in {
    val mockDirectives = new MockAuditDirectives()
    Get("/") ~> mockDirectives.auditRequest(complete(MockResult("ok"))) ~> check {
      responseAs[MockResult] should be(MockResult("ok"))
    }
  }

  it should "auditResult" in {
    val mockDirectives = new MockAuditDirectives()
    Get("/") ~> mockDirectives.auditResult(complete(MockResult("ok"))) ~> check {
      responseAs[MockResult] should be(MockResult("ok"))
    }
  }

  it should "auditException" in {
    implicit val exceptionHandler = ExceptionHandler {
      case MockAuditDAO.ExceptionMock => complete(MockResult("ok"))
    }
    val mockDirectives = new MockAuditDirectives()
    Get("/") ~> mockDirectives.auditException(throw MockAuditDAO.ExceptionMock) ~> check {
      responseAs[MockResult] should be(MockResult("ok"))
    }
  }

  it should "fail when auditRequest fails" in {
    val failingAuditDAO = new FailingAuditDAO()
    val mockDirectives = new MockAuditDirectives(
      auditDAO = failingAuditDAO
    )
    Get("/") ~> mockDirectives.auditRequest(complete(MockResult("ok"))) ~> check {
      response.status.isFailure() should be(true)
    }
  }

  it should "fail when auditResult fails" in {
    val failingAuditDAO = new FailingAuditDAO()
    val mockDirectives = new MockAuditDirectives(
      auditDAO = failingAuditDAO
    )

    Get("/") ~> mockDirectives.auditResult(complete(MockResult("ok"))) ~> check {
      response.status.isFailure() should be(true)
    }
  }

  it should "fail when auditException fails" in {
    val failingAuditDAO = new FailingAuditDAO()
    val mockDirectives = new MockAuditDirectives(
      auditDAO = failingAuditDAO
    )

    Get("/") ~> mockDirectives.auditException(throw MockAuditDAO.ExceptionMock) ~> check {
      response.status.isFailure() should be(true)
    }
  }
}
