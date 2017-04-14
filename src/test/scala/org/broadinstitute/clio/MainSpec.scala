package org.broadinstitute.clio

import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.scalatest.{FlatSpec, Matchers}

class MainSpec extends FlatSpec with Matchers with ScalatestRouteTest {
  behavior of "Main"

  it should "get /hello" in {
    Get("/hello") ~> Main.exampleRoute ~> check {
      responseAs[String] shouldEqual
        s"""|{
            |  "message" : "Say hello to Clio ${Main.version}"
            |}
            |""".stripMargin.trim
    }
  }

  it should "not get /" in {
    Get() ~> Main.exampleRoute ~> check {
      handled shouldBe false
    }
  }
}
