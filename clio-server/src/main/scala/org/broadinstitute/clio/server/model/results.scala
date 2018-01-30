package org.broadinstitute.clio.server.model

import java.time.OffsetDateTime

import akka.http.javadsl.server.CustomRejection

case class ErrorResult(error: String)

case class RejectionResult(rejection: String)

object ApiNotReadyRejection extends CustomRejection

case class ApiNotReadyResult(message: String, serverStartTime: OffsetDateTime)
