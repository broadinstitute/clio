package org.broadinstitute.clio.client.webclient

import akka.http.scaladsl.model.headers.HttpCredentials

/** Interface for objects which can generate HTTP credentials for authentication with Clio. */
trait CredentialsGenerator {
  def generateCredentials(): HttpCredentials
}
