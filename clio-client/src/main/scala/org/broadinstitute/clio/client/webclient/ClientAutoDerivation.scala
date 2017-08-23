package org.broadinstitute.clio.client.webclient

import io.circe.Printer
import org.broadinstitute.clio.util.json.ModelAutoDerivation

/** Automatic JSON encoding and decoding for the client. */
object ClientAutoDerivation extends ModelAutoDerivation {

  /**
    * Formats the webservice outputs pretty with two spaces.
    *
    * https://github.com/hseeberger/akka-http-json/blob/v1.17.0/akka-http-circe/src/main/scala/de/heikoseeberger/akkahttpcirce/CirceSupport.scala#L76-L97
    */
  implicit val akkaHttpJsonPrinter: Printer =
    Printer.spaces2.copy(dropNullKeys = true)
}