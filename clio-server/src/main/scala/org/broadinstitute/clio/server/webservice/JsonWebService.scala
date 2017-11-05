package org.broadinstitute.clio.server.webservice

import akka.http.scaladsl.common.{
  EntityStreamingSupport,
  JsonEntityStreamingSupport
}
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport
import org.broadinstitute.clio.util.json.ModelAutoDerivation

/**
  * Utility trait containing settings / implicits used by web-service
  * classes to read / write JSON.
  */
trait JsonWebService
    extends ModelAutoDerivation
    with ErrorAccumulatingCirceSupport {

  /**
    * Settings configuring how web-service classes will render JSON
    * streams into HTTP responses.
    *
    * By default, JSON streams are sent to the client as valid JSON
    * arrays, with elements marshalled into byte strings one-by-one.
    *
    * We override the config so multiple stream elements can be marshalled
    * in parallel, but maintain the array structure and ordering to avoid
    * breaking anything downstream.
    *
    * More customizations are possible, see:
    * https://doc.akka.io/docs/akka-http/current/scala/http/routing-dsl/source-streaming-support.html#customising-response-rendering-mode
    */
  final implicit val jsonStreamingSupport: JsonEntityStreamingSupport =
    EntityStreamingSupport
      .json()
      .withParallelMarshalling(
        parallelism = Runtime.getRuntime.availableProcessors() / 2,
        unordered = false
      )
}
