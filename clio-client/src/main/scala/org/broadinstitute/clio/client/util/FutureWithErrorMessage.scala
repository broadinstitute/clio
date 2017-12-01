package org.broadinstitute.clio.client.util

import com.typesafe.scalalogging.{LazyLogging, Logger}

import scala.concurrent.{ExecutionContext, Future}

trait FutureWithErrorMessage extends LazyLogging {
  implicit val implicitLogger: Logger = logger
  implicit class FutureWithErrorMessage[A](future: Future[A]) {

    def logErrorMsg(
      additionalMessage: String
    )(implicit ec: ExecutionContext): Future[A] = {
      future recover {
        case ex =>
          throw new Exception(additionalMessage, ex)
      }
    }
  }
}
