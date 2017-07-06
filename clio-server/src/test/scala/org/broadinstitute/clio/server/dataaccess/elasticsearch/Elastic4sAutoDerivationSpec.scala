package org.broadinstitute.clio.server.dataaccess.elasticsearch

import io.circe.syntax._
import org.broadinstitute.clio.server.dataaccess.elasticsearch.Elastic4sAutoDerivation._
import org.scalatest.{FlatSpec, Matchers}

class Elastic4sAutoDerivationSpec extends FlatSpec with Matchers {
  behavior of "Elastic4sAutoDerivation"

  it should "implicitly drop null keys into a compact string" in {
    case class TestClass(fieldA: Option[String], fieldB: Int)

    val instance = TestClass(None, 0)
    Elastic4sAutoDerivation.implicitEncoder(instance.asJson) should be(
      """{"field_b":0}"""
    )
  }
}
