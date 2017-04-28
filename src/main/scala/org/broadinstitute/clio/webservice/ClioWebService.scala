package org.broadinstitute.clio.webservice

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

/**
  * Common mixin for a webservice.
  */
trait ClioWebService {
  implicit def system: ActorSystem

  implicit def materializer: ActorMaterializer
}
