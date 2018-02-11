package org.broadinstitute.clio.util.json

import java.net.URI
import java.time.OffsetDateTime

import enumeratum._
import io.circe.parser._
import io.circe.syntax._
import org.broadinstitute.clio.util.model.UpsertId
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{EitherValues, FlatSpec, Matchers}

import scala.collection.immutable.IndexedSeq

class ModelAutoDerivationSpec
    extends FlatSpec
    with Matchers
    with TableDrivenPropertyChecks
    with EitherValues
    with ModelAutoDerivation {
  behavior of "ModelAutoDerivation"

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
      input.asJson.pretty(defaultPrinter) should be(expected)
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
    test.asJson.pretty(defaultPrinter) should be(
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

  it should "show the string which caused the error when failing to decode a date" in {
    case class TestClass(date: OffsetDateTime)

    val malformed = "not-a-date"
    val json = s"""{"date":"$malformed"}"""

    import cats.syntax.show._

    decode[TestClass](json).left.value.show should include(malformed)
  }

  it should "encode an enum" in {
    import ModelAutoDerivationSpec._
    case class TestClass(enum: TestEnum)

    val test = TestClass(TestEnum.TestValue1)
    test.asJson.pretty(defaultPrinter) should be("""{"enum":"TestValue1"}""")
  }

  it should "decode an enum" in {
    import ModelAutoDerivationSpec._
    case class TestClass(enum: TestEnum)

    val json = """{"enum":"TestValue2"}"""
    decode[TestClass](json).right.value should be(
      TestClass(TestEnum.TestValue2)
    )
  }

  it should "show the string which caused the error when failing to decode an enum" in {
    import ModelAutoDerivationSpec._
    case class TestClass(enum: TestEnum)

    import cats.syntax.show._

    val malformed = "TestValue3"
    val json = s"""{"enum": "$malformed"}"""

    decode[TestClass](json).left.value.show should include(malformed)
  }

  it should "encode a URI" in {
    case class TestClass(uri: URI)

    val uriValues = Table(
      "uri",
      "/seq/picard/some/file/path.stuff",
      "gs://broad-gotc-dev-storage/some/file/path.stuff"
    )

    forAll(uriValues) { uri =>
      TestClass(URI.create(uri)).asJson.pretty(defaultPrinter) should be(
        s"""{"uri":"$uri"}"""
      )
    }
  }

  it should "decode a URI" in {
    case class TestClass(uri: URI)

    val uriValues = Table(
      "uri",
      "/seq/picard/some/file/path.stuff",
      "gs://some-bucket/some/file/path.stuff"
    )

    forAll(uriValues) { uri =>
      decode[TestClass](s"""{"uri":"$uri"}""").right.value should be(
        TestClass(URI.create(uri))
      )
    }
  }

  it should "show the string which caused the error when failing to decode a URI" in {
    case class TestClass(uri: URI)

    import cats.syntax.show._

    val malformed = "*&^)"
    val json = s"""{"uri":"$malformed"}"""

    decode[TestClass](json).left.value.show should include(malformed)
  }

  it should "encode an UpsertID" in {
    case class TestClass(id: UpsertId)

    val id = UpsertId.nextId()
    TestClass(id).asJson.pretty(defaultPrinter) should be(s"""{"id":"${id.id}"}""")
  }

  it should "decode an UpsertID" in {
    case class TestClass(id: UpsertId)

    val id = UpsertId.nextId()
    decode[TestClass](s"""{"id":"${id.id}"}""").right.value should be(TestClass(id))
  }

  it should "show the string which caused the error when failing to decode an UpsertId" in {
    case class TestClass(id: UpsertId)

    import cats.syntax.show._

    val malformed = "123badId"
    val json = s"""{"id":"$malformed"}"""

    decode[TestClass](json).left.value.show should include(malformed)
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
