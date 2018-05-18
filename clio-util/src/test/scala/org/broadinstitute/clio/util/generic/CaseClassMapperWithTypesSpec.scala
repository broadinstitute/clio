package org.broadinstitute.clio.util.generic

import org.scalatest.{FlatSpec, Matchers}

import scala.reflect.runtime.universe.typeOf

class CaseClassMapperWithTypesSpec extends FlatSpec with Matchers {
  behavior of "CaseClassMapperWithTypes"

  import CaseClassMapperWithTypesSpec._

  it should "get scala types" in {
    val expected =
      Map("fieldA" -> typeOf[Option[String]], "fieldB" -> typeOf[Int])
    val mapper = new CaseClassMapperWithTypes[TestClass]
    for (key <- expected.keys) {
      assert(mapper(key) =:= expected(key))
    }
  }

}

object CaseClassMapperWithTypesSpec {

  case class TestClass(fieldA: Option[String], fieldB: Int)

}
