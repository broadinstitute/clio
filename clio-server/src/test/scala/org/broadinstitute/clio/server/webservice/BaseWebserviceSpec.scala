package org.broadinstitute.clio.server.webservice

import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.broadinstitute.clio.util.json.DecodingUtil
import org.scalatest.{FlatSpec, Matchers}

/**
  * Base class mixing in traits common to all webservice / directive tests.
  */
abstract class BaseWebserviceSpec
    extends FlatSpec
    with Matchers
    with ScalatestRouteTest
    with JsonWebService
    with DecodingUtil {}
