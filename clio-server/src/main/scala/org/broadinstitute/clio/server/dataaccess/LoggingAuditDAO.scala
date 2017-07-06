package org.broadinstitute.clio.server.dataaccess

import com.typesafe.scalalogging.StrictLogging
import org.broadinstitute.clio.model.{ClioRequest, ClioResponse}
import org.slf4j.MarkerFactory

import scala.concurrent.{ExecutionContext, Future}

class LoggingAuditDAO private (implicit executionContext: ExecutionContext)
    extends AuditDAO
    with StrictLogging {
  override def auditRequest(request: ClioRequest): Future[Unit] = {
    Future {
      logger.info(LoggingAuditDAO.AuditMarker, s"${request.content}")
    }
  }

  override def auditResponse(request: ClioRequest,
                             response: ClioResponse): Future[Unit] = {
    Future {
      logger.info(
        LoggingAuditDAO.AuditMarker,
        s"${request.content} -> ${response.content}"
      )
    }
  }

  override def auditException(request: ClioRequest,
                              exception: Exception): Future[Unit] = {
    Future {
      logger.error(
        LoggingAuditDAO.AuditMarker,
        s"${request.content} -> $exception",
        exception
      )
    }
  }
}

object LoggingAuditDAO {
  def apply()(implicit executionContext: ExecutionContext): AuditDAO = {
    new LoggingAuditDAO
  }

  private val AuditMarker = MarkerFactory.getMarker("AUDIT")
}
