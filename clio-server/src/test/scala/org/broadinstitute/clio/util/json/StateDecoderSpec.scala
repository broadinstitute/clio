package org.broadinstitute.clio.util.json

import org.scalatest.{EitherValues, FlatSpec, Matchers}
import io.circe._
import io.circe.parser._
import org.scalatest.prop.TableDrivenPropertyChecks

class StateDecoderSpec
    extends FlatSpec
    with Matchers
    with EitherValues
    with TableDrivenPropertyChecks {
  behavior of "StateDecoder"

  it should "decode an empty class" in {
    case class TestClass()

    implicit val decoder = Decoder.fromState(StateDecoder[TestClass].state)
    decode[TestClass]("""{}""").right.value should be(TestClass())
  }

  it should "decode a field with options" in {
    case class TestClass(fieldA: Option[String], fieldB: Int)

    val jsonValues =
      Table(
        ("input", "expected"),
        (
          """{"fieldA": "hello", "fieldB": 123}""",
          TestClass(Option("hello"), 123)
        ),
        ("""{"fieldB": 456}""", TestClass(None, 456))
      )

    implicit val decoder = Decoder.fromState(StateDecoder[TestClass].state)
    forAll(jsonValues) { (input, expected) =>
      decode[TestClass](input).right.value should be(expected)
    }
  }

  it should "fail to decode bad json" in {
    case class TestClass(fieldA: Option[String], fieldB: Int)

    val jsonValues =
      Table(
        ("input", "expected"),
        (
          """{"fieldA": "hello", "fieldB": "world"}""",
          """DecodingFailure at .fieldA{!_/}.fieldB: Int"""
        ),
        (
          """{"fieldA": "hello", "fieldB": 123, "fieldC": false}""",
          """DecodingFailure at .fieldA{!_/}.fieldB{!_/}: Leftover keys: fieldC"""
        ),
        (
          """{"fieldA": "hello"}""",
          """DecodingFailure at .fieldA{!_/}.fieldB: Attempt to decode value on failed cursor"""
        )
      )

    import cats.syntax.show._

    implicit val decoder = Decoder.fromState(StateDecoder[TestClass].state)
    forAll(jsonValues) { (input, expected) =>
      decode[TestClass](input).left.value.show should be(expected)
    }
  }
}
