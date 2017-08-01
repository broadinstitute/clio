package org.broadinstitute.clio.util.json

import java.time.OffsetDateTime

import enumeratum._
import io.circe.Printer
import io.circe.parser._
import io.circe.syntax._
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{EitherValues, FlatSpec, Matchers}

import scala.collection.immutable.IndexedSeq

class ModelAutoDerivationSpec
    extends FlatSpec
    with Matchers
    with TableDrivenPropertyChecks
    with EitherValues {
  behavior of "ModelAutoDerivation"

  object TestModelAutoDerivation extends ModelAutoDerivation

  import TestModelAutoDerivation._

  private val noNullPrinter = Printer.noSpaces.copy(dropNullKeys = true)

  it should "encode with snake case" in {
    case class TestClass(fieldA: Option[String], fieldB: Int)

    val jsonValues =
      Table(
        ("input", "expected"),
        (
          TestClass(Option("hello"), 123),
          """{"field_a":"hello","field_b":123}"""
        ),
        (TestClass(None, 456), """{"field_b":456}""")
      )

    forAll(jsonValues) { (input, expected) =>
      input.asJson.pretty(noNullPrinter) should be(expected)
    }

  }

  it should "decode snake case" in {
    case class TestClass(fieldA: Option[String], fieldB: Int)

    val jsonValues =
      Table(
        ("input", "expected"),
        (
          """{"field_a": "hello", "field_b": 123}""",
          TestClass(Option("hello"), 123)
        ),
        ("""{"field_b": 456}""", TestClass(None, 456))
      )

    forAll(jsonValues) { (input, expected) =>
      decode[TestClass](input).right.value should be(expected)
    }
  }

  it should "fail to decode camel case" in {
    case class TestClass(fieldA: Option[String], fieldB: Int)

    val jsonValues =
      Table(
        ("input", "expected"),
        (
          """{"fieldA": "hello", "field_b": 123}""",
          """DecodingFailure at .field_b{!_/}: Leftover keys: fieldA"""
        ),
        (
          """{"fieldB": 456}""",
          """DecodingFailure at .field_b: Attempt to decode value on failed cursor"""
        )
      )

    import cats.syntax.show._

    forAll(jsonValues) { (input, expected) =>
      decode[TestClass](input).left.value.show should be(expected)
    }
  }

  it should "encode a date" in {
    case class TestClass(date: OffsetDateTime)

    val test = TestClass(OffsetDateTime.parse("1970-01-01T12:34:56.789+05:00"))
    test.asJson.pretty(noNullPrinter) should be(
      """{"date":"1970-01-01T12:34:56.789+05:00"}"""
    )
  }

  it should "decode a date" in {
    case class TestClass(date: OffsetDateTime)

    val json = """{"date":"1970-01-01T12:34:56.789+05:00"}"""
    decode[TestClass](json).right.value should be(
      TestClass(OffsetDateTime.parse("1970-01-01T12:34:56.789+05:00"))
    )
  }

  it should "encode an enum" in {
    import ModelAutoDerivationSpec._
    case class TestClass(enum: TestEnum)

    val test = TestClass(TestEnum.TestValue1)
    test.asJson.pretty(noNullPrinter) should be("""{"enum":"TestValue1"}""")
  }

  it should "decode an enum" in {
    import ModelAutoDerivationSpec._
    case class TestClass(enum: TestEnum)

    val json = """{"enum":"TestValue2"}"""
    decode[TestClass](json).right.value should be(
      TestClass(TestEnum.TestValue2)
    )
  }
}

object ModelAutoDerivationSpec {

  sealed trait TestEnum extends EnumEntry

  object TestEnum extends Enum[TestEnum] {
    override def values: IndexedSeq[TestEnum] = findValues

    case object TestValue1 extends TestEnum

    case object TestValue2 extends TestEnum

  }

}
