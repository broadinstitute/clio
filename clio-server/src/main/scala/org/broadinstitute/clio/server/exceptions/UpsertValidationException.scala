package org.broadinstitute.clio.server.exceptions

case class UpsertValidationException(message: String) extends Exception(message)
