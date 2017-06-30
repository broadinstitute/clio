package org.broadinstitute.clio.webservice

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.RouteResult.{Complete, Rejected}
import akka.http.scaladsl.server._
import org.broadinstitute.clio.model.{ClioRequest, ClioResponse}
import org.broadinstitute.clio.service.AuditService

/** An audit trail of request and responses. */
trait AuditDirectives {

  def auditService: AuditService

  /** Audits the request. */
  lazy val auditRequest: Directive0 = {
    extractRequestContext flatMap { requestContext =>
      val request = toClioRequest(requestContext)
      auditService.auditRequest(request)
      pass
    }
  }

  /** Audits the request result. */
  lazy val auditResult: Directive0 = {
    extractRequestContext flatMap { requestContext =>
      mapRouteResult { routeResult =>
        val request = toClioRequest(requestContext)
        val response = toClioResponse(routeResult)
        auditService.auditResponse(request, response)
        routeResult
      }
    }
  }

  /** Audits an exception, and then re-throws the exception. */
  lazy val auditException: Directive0 = {
    extractRequestContext flatMap { requestContext =>
      handleExceptions(auditExceptionHandler(requestContext))
    }
  }

  private def toClioRequest(requestContext: RequestContext): ClioRequest = {
    ClioRequest(requestContext.request.toString)
  }

  private def toClioResponse(routeResult: RouteResult): ClioResponse = {
    routeResult match {
      case Complete(httpResponse) => ClioResponse(httpResponse.toString)
      case Rejected(rejections) =>
        ClioResponse(
          if (rejections.isEmpty) "Rejected" else rejections.mkString(", ")
        )
    }
  }

  private def auditExceptionHandler(
    requestContext: RequestContext
  ): ExceptionHandler = {
    ExceptionHandler {
      case exception: Exception =>
        val request = toClioRequest(requestContext)
        auditService.auditException(request, exception)
        throw exception
    }
  }
}
