package org.broadinstitute.clio.util.json

import io.circe._
import io.circe.parser._
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{EitherValues, FlatSpec, Matchers}

class StateDecoderSpec
    extends FlatSpec
    with Matchers
    with EitherValues
    with TableDrivenPropertyChecks {
  behavior of "StateDecoder"

  case class EmptyClass()
  case class TestClass(fieldA: Option[String], fieldB: Int)

  it should "decode an empty class" in {
    implicit val decoder: Decoder[EmptyClass] =
      Decoder.fromState(StateDecoder[EmptyClass].state)
    decode[EmptyClass]("""{}""").right.value should be(EmptyClass())
  }

  it should "decode a field with options" in {

    val jsonValues =
      Table(
        ("input", "expected"),
        (
          """{"fieldA": "hello", "fieldB": 123}""",
          TestClass(Option("hello"), 123)
        ),
        ("""{"fieldB": 456}""", TestClass(None, 456))
      )

    implicit val decoder: Decoder[TestClass] =
      Decoder.fromState(StateDecoder[TestClass].state)
    forAll(jsonValues) { (input, expected) =>
      decode[TestClass](input).right.value should be(expected)
    }
  }

  it should "decode a field with an option explicitly set to null" in {
    implicit val decoder: Decoder[TestClass] =
      Decoder.fromState(StateDecoder[TestClass].state)

    decode[TestClass]("""{"fieldA": null, "fieldB": 123}""").right.value should be(
      TestClass(None, 123)
    )
  }

  it should "fail to decode bad json" in {

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

    implicit val decoder: Decoder[TestClass] =
      Decoder.fromState(StateDecoder[TestClass].state)
    forAll(jsonValues) { (input, expected) =>
      decode[TestClass](input).left.value.show should be(expected)
    }
  }
}
