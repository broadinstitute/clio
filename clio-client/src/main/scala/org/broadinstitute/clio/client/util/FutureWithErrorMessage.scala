package org.broadinstitute.clio.client.util

import com.typesafe.scalalogging.Logger

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object FutureWithErrorMessage {
  implicit class FutureWithErrorMessage[A](future: Future[A]) {
    def withErrorMsg(message: String)(implicit ec: ExecutionContext,
                                      logger: Logger): Future[A] = {
      future andThen {
        case Success(a) => Future.successful(a)
        case Failure(ex) =>
          logger.error(message)
          Future.failed(new Exception(message, ex))
      }
    }
  }
}
