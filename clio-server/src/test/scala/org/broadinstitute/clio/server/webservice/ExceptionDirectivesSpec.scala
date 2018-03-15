package org.broadinstitute.clio.server.webservice

import akka.http.scaladsl.server.Directives._
import org.broadinstitute.clio.server.model.{ErrorResult, MockResult}

class ExceptionDirectivesSpec extends BaseWebserviceSpec {
  behavior of "ExceptionDirectives"

  it should "complete with json on internal error" in {
    val exceptionDirectives = new ExceptionDirectives
    Get("/") ~> exceptionDirectives.completeWithInternalErrorJson(
      complete(throw new RuntimeException("expected"))
    ) ~> check {
      responseAs[ErrorResult] should be(
        ErrorResult("There was an internal server error.")
      )
    }
  }

  it should "complete normally when no error" in {
    val exceptionDirectives = new ExceptionDirectives
    Get("/") ~> exceptionDirectives.completeWithInternalErrorJson(
      complete(MockResult("ok"))
    ) ~> check {
      responseAs[MockResult] should be(MockResult("ok"))
    }
  }
}
