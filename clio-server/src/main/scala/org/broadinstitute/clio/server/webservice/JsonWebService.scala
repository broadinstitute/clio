package org.broadinstitute.clio.server.webservice

import akka.http.scaladsl.common.{
  EntityStreamingSupport,
  JsonEntityStreamingSupport
}
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport
import org.broadinstitute.clio.util.json.ModelAutoDerivation

/**
  * Utility trait for combining mixins used across web-service classes.
  */
trait JsonWebService
    extends ModelAutoDerivation
    with ErrorAccumulatingCirceSupport {

  final implicit val jsonStreamingSupport: JsonEntityStreamingSupport =
    EntityStreamingSupport
      .json()
      .withParallelMarshalling(
        parallelism = Runtime.getRuntime.availableProcessors() / 2,
        unordered = false
      )
}
