package org.broadinstitute.clio.server.dataaccess.elasticsearch

import io.circe.Json
import io.circe.syntax._
import org.scalatest.{FlatSpec, Matchers}

class Elastic4sAutoDerivationSpec
    extends FlatSpec
    with Matchers
    with Elastic4sAutoDerivation {
  behavior of "Elastic4sAutoDerivation"

  it should "implicitly drop null keys into a compact string" in {
    case class TestClass(fieldA: Option[String], fieldB: Int)

    val instance = TestClass(None, 0)
    // Mixing-in `Elastic4sAutoDerivation` should provide an implicit Json printer.
    implicitly[Json => String].apply(instance.asJson) should be(
      """{"field_b":0}"""
    )
  }
}
