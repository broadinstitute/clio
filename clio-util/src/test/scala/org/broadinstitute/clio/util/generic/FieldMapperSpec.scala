package org.broadinstitute.clio.util.generic

import org.scalatest.{FlatSpec, Matchers}

import scala.reflect.runtime.universe.typeOf

class FieldMapperSpec extends FlatSpec with Matchers {
  behavior of "FieldMapper"

  it should "map the fields of a case class" in {
    case class TestClass(fieldA: Option[String], fieldB: Int, fieldC: Boolean)

    val expected = Map(
      "fieldA" -> typeOf[Option[String]],
      "fieldB" -> typeOf[Int],
      "fieldC" -> typeOf[Boolean]
    )
    val fieldMapper = FieldMapper[TestClass]
    fieldMapper.keys should contain theSameElementsAs expected.keys
    for (key <- expected.keys) {
      assert(fieldMapper.typeOf(key) =:= expected(key))
    }
  }
}
