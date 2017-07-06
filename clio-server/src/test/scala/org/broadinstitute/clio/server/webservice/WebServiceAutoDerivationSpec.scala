package org.broadinstitute.clio.server.webservice

import io.circe.Printer
import io.circe.syntax._
import org.broadinstitute.clio.server.webservice.WebServiceAutoDerivation._
import org.scalatest.{FlatSpec, Matchers}

class WebServiceAutoDerivationSpec extends FlatSpec with Matchers {
  behavior of "WebServiceAutoDerivation"

  it should "implicitly drop null keys into a pretty string" in {
    case class TestClass(fieldA: Option[String], fieldB: Int)

    val instance = TestClass(None, 0)
    val printer = implicitly[Printer]
    instance.asJson.pretty(printer) should be("""|{
                                                 |  "field_b" : 0
                                                 |}""".stripMargin)
  }
}
