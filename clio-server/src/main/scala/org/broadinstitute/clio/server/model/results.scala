package org.broadinstitute.clio.server.model

import akka.http.javadsl.server.CustomRejection

case class ErrorResult(error: String)

case class RejectionResult(rejection: String)

object ApiNotReadyRejection extends CustomRejection
