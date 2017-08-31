package org.broadinstitute.clio.client.util

import com.typesafe.scalalogging.Logger

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

trait FutureWithErrorMessage {
  implicit class FutureWithErrorMessage[A](future: Future[A]) {
    def withErrorMsg(message: String = "")(implicit ec: ExecutionContext,
                                           logger: Logger): Future[A] = {
      future andThen {
        case Success(_) => ()
        case Failure(ex) =>
          if (Option(ex.getMessage).isDefined)
            logger.error(ex.getMessage)
          if (!message.isEmpty)
            logger.error(message)
      }
    }
  }
}
