package org.broadinstitute.clio.client.util

import com.typesafe.scalalogging.{LazyLogging, Logger}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

trait FutureWithErrorMessage extends LazyLogging {
  implicit val implicitLogger: Logger = logger
  implicit class FutureWithErrorMessage[A](future: Future[A]) {
    def logErrorMsg(
        additionalMessage: String = ""
    )(implicit ec: ExecutionContext, logger: Logger): Future[A] = {
      future andThen {
        case Success(_) => ()
        case Failure(ex) =>
          if (Option(ex.getMessage).isDefined)
            logger.error(ex.getMessage)
          if (!additionalMessage.isEmpty)
            logger.error(additionalMessage)
      }
    }
  }
}
