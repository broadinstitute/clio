package org.broadinstitute.clio.server.webservice

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.testkit.ScalatestRouteTest
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import io.circe.generic.auto._
import org.broadinstitute.clio.model.ErrorResult
import org.broadinstitute.clio.server.model.MockResult
import org.scalatest.{FlatSpec, Matchers}

class ExceptionDirectivesSpec
    extends FlatSpec
    with Matchers
    with ScalatestRouteTest {
  behavior of "ExceptionDirectives"

  it should "complete with json on internal error" in {
    val mockDirectives = MockExceptionDirectives
    Get("/") ~> mockDirectives.completeWithInternalErrorJson(
      complete(throw new RuntimeException("expected"))
    ) ~> check {
      responseAs[ErrorResult] should be(
        ErrorResult("There was an internal server error.")
      )
    }
  }

  it should "complete normally when no error" in {
    val mockDirectives = MockExceptionDirectives
    Get("/") ~> mockDirectives.completeWithInternalErrorJson(
      complete(MockResult("ok"))
    ) ~> check {
      responseAs[MockResult] should be(MockResult("ok"))
    }
  }
}
