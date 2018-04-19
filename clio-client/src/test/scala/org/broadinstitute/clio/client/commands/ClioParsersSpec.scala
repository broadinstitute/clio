package org.broadinstitute.clio.client.commands

import java.time.OffsetDateTime

import caseapp.core.Error
import caseapp.core.argparser.ArgParser
import enumeratum.{Enum, EnumEntry}
import org.broadinstitute.clio.util.model.{DataType, DocumentStatus, Location}
import org.scalatest.{FlatSpec, Matchers}

import scala.reflect.ClassTag

class ClioParsersSpec extends FlatSpec with Matchers with ClioParsers {

  Location.values.foreach {
    it should behave like enumParser(_)
  }
  DocumentStatus.values.foreach {
    it should behave like enumParser(_)
  }
  DataType.values.foreach {
    it should behave like enumParser(_)
  }

  it should behave like enumParserErr[Location]
  it should behave like enumParserErr[DocumentStatus]
  it should behave like enumParserErr[DataType]

  def enumParser[E <: EnumEntry: Enum](entry: E)(implicit c: ClassTag[E]): Unit = {
    val stringified = entry.entryName

    it should s"properly parse $stringified as an instance of ${c.runtimeClass.getName}" in {
      implicitly[ArgParser[E]].apply(None, stringified) should be(Right(entry))
    }
  }

  def enumParserErr[E <: EnumEntry](implicit e: Enum[E], c: ClassTag[E]): Unit = {
    it should s"raise an error when parsing an invalid ${c.runtimeClass.getName}" in {
      implicitly[ArgParser[E]].apply(None, "fklasdjf;lidasjvkl;daj") match {
        case Right(_) => fail("Enum parser unexpectedly parsed bad value")
        case Left(err) =>
          e.values.foreach { v =>
            err.message should include(v.entryName)
          }
      }
    }
  }

  it should "properly parse an OffsetDateTime" in {
    val now = OffsetDateTime.now()
    implicitly[ArgParser[OffsetDateTime]].apply(None, now.toString) should be(Right(now))
  }

  it should "fail to parse a malformed OffsetDateTime" in {
    implicitly[ArgParser[OffsetDateTime]]
      .apply(None, "agajvdssav") shouldBe a[Left[_, _]]
  }

  it should "error out when there are double quotes on the ends of String inputs" in {
    implicitly[ArgParser[String]].apply(None, "\"sampleAlias\"") should matchPattern {
      case Left(Error.MalformedValue("string", "Quotes are not allowed in inputs")) =>
    }
  }

  it should "error out when there are double quotes in the middle of String inputs" in {
    implicitly[ArgParser[String]].apply(
      None,
      "someone said \"something\" at some point in time"
    ) should matchPattern {
      case Left(Error.MalformedValue("string", "Quotes are not allowed in inputs")) =>
    }
  }

  it should "parse single quotes in String inputs" in {
    val in = "I can\'t let you do that, Dave"
    implicitly[ArgParser[String]].apply(None, in) should be(Right(in))
  }
}
