package org.broadinstitute.clio.server.webservice

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.RouteResult.{Complete, Rejected}
import akka.http.scaladsl.server._
import org.broadinstitute.clio.server.model.{ClioRequest, ClioResponse}
import org.broadinstitute.clio.server.service.AuditService

/** An audit trail of request and responses. */
class AuditDirectives(auditService: AuditService) {

  /** Audits the request. */
  val auditRequest: Directive0 = {
    extractRequestContext flatMap { requestContext =>
      val request = toClioRequest(requestContext)
      onSuccess(auditService.auditRequest(request))
    }
  }

  /** Audits the request result. */
  val auditResult: Directive0 = {
    extractRequestContext flatMap { requestContext =>
      mapRouteResultFuture { routeResultFut =>
        import requestContext.executionContext
        val request = toClioRequest(requestContext)
        for {
          routeResult <- routeResultFut
          response = toClioResponse(routeResult)
          _ <- auditService.auditResponse(request, response)
        } yield routeResult
      }
    }
  }

  /** Audits an exception, and then re-throws the exception. */
  val auditException: Directive0 = {
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
        onSuccess(auditService.auditException(request, exception)) {
          throw exception
        }
    }
  }
}
