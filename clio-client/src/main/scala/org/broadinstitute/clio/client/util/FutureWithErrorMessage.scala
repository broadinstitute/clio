package org.broadinstitute.clio.client.util

import com.typesafe.scalalogging.Logger

import scala.concurrent.{ExecutionContext, Future}

object FutureWithErrorMessage {
  implicit class FutureWithErrorMessage[A](future: Future[A]) {
    def withErrorMsg(message: String)(implicit ec: ExecutionContext,
                                      logger: Logger): Future[A] = {
      future recover {
        case ex =>
          logger.error(message)
          throw new Exception(message, ex)
      }
    }
  }
}
