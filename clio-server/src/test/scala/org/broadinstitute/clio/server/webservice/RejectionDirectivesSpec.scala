package org.broadinstitute.clio.server.webservice

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.testkit.ScalatestRouteTest
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import io.circe.generic.auto._
import org.broadinstitute.clio.model.RejectionResult
import org.scalatest.{FlatSpec, Matchers}

class RejectionDirectivesSpec
    extends FlatSpec
    with Matchers
    with ScalatestRouteTest {
  behavior of "RejectionDirectives"

  it should "map default rejections to json" in {
    Get("/") ~> MockRejectionDirectives.mapRejectionsToJson(reject) ~> check {
      responseAs[RejectionResult] should be(
        RejectionResult("The requested resource could not be found.")
      )
    }
  }

  it should "map invalid HTTP method rejections to json" in {
    Get("/") ~> MockRejectionDirectives.mapRejectionsToJson(post(reject)) ~> check {
      responseAs[RejectionResult] should be(
        RejectionResult("HTTP method not allowed, supported methods: POST")
      )
    }
  }

  it should "not map completions" in {
    Get("/") ~> MockRejectionDirectives.mapRejectionsToJson(
      complete(MockResult("ok"))
    ) ~> check {
      responseAs[MockResult] should be(MockResult("ok"))
    }
  }
}
