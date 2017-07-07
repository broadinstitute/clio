package org.broadinstitute.clio.util.generic

import org.scalatest.{FlatSpec, Matchers}

class CaseClassMapperSpec extends FlatSpec with Matchers {
  behavior of "CaseClassMapper"

  import CaseClassMapperSpec._

  it should "get field names" in {
    val mapper = new CaseClassMapper[TestClass]
    mapper.names should contain theSameElementsInOrderAs Seq(
      "fieldA",
      "fieldB"
    )
  }

  it should "get field values" in {
    val mapper = new CaseClassMapper[TestClass]

    val instance = TestClass(None, 0)
    mapper.vals(instance) should contain theSameElementsAs Map(
      "fieldA" -> None,
      "fieldB" -> 0
    )
  }

  it should "create a new instance" in {
    val mapper = new CaseClassMapper[TestClass]
    val vals = Map("fieldA" -> None, "fieldB" -> 0)
    mapper.newInstance(vals) should be(TestClass(None, 0))
  }

  it should "fail to create a new instance missing values" in {
    val mapper = new CaseClassMapper[TestClass]
    val vals = Map("fieldA" -> None)
    val exception =
      intercept[IllegalArgumentException](mapper.newInstance(vals))
    exception.getMessage should be(
      s"Missing field fieldB: java.lang.Integer for org.broadinstitute.clio.util.generic.CaseClassMapperSpec$$TestClass"
    )
  }

  it should "fail to create a new instance with wrong types" in {
    val mapper = new CaseClassMapper[TestClass]
    val vals = Map("fieldA" -> None, "fieldB" -> 1.23)
    val exception =
      intercept[IllegalArgumentException](mapper.newInstance(vals))
    exception.getMessage should be(
      "Value '1.23' of type java.lang.Double cannot be assigned to field fieldB: java.lang.Integer"
    )
  }

  it should "fail to create a new instance with extra fields" in {
    val mapper = new CaseClassMapper[TestClass]
    val vals = Map("fieldA" -> None, "fieldB" -> 0, "fieldC" -> true)
    val exception =
      intercept[IllegalArgumentException](mapper.newInstance(vals))
    exception.getMessage should be(
      s"""|Unknown val for org.broadinstitute.clio.util.generic.CaseClassMapperSpec$$TestClass:
          |  fieldC -> true""".stripMargin
    )
  }

  it should "copy values into an existing instance" in {
    val mapper = new CaseClassMapper[TestClass]
    val instance = TestClass(Option("hello"), 0)
    val vals = Map("fieldB" -> 123)
    mapper.copy(instance, vals) should be(TestClass(Option("hello"), 123))
  }

  it should "fail to copy values into an existing instance with wrong types" in {
    val mapper = new CaseClassMapper[TestClass]
    val instance = TestClass(None, 0)
    val vals = Map("fieldB" -> 1.23)
    val exception =
      intercept[IllegalArgumentException](mapper.copy(instance, vals))
    exception.getMessage should be(
      "Value '1.23' of type java.lang.Double cannot be assigned to field fieldB: java.lang.Integer"
    )
  }

  it should "fail to copy values into an existing instance with extra fields" in {
    val mapper = new CaseClassMapper[TestClass]
    val instance = TestClass(None, 0)
    val vals = Map("fieldC" -> true)
    val exception =
      intercept[IllegalArgumentException](mapper.copy(instance, vals))
    exception.getMessage should be(
      s"""|Unknown val for org.broadinstitute.clio.util.generic.CaseClassMapperSpec$$TestClass:
          |  fieldC -> true""".stripMargin
    )
  }

  it should "fail to map an inner class" in {
    case class InnerClass(a: String)
    val exception = intercept[NoSuchMethodException] {
      new CaseClassMapper[InnerClass]
    }
    exception.getMessage should fullyMatch regex
      "org.broadinstitute.clio.util.generic.CaseClassMapperSpec.InnerClass.*<init>" +
        "\\(java.lang.String, org.broadinstitute.clio.util.generic.CaseClassMapperSpec\\)"
  }
}

object CaseClassMapperSpec {

  case class TestClass(fieldA: Option[String], fieldB: Int)

}
