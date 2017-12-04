package org.broadinstitute.clio.util.generic

import org.scalatest.{FlatSpec, Matchers}

class CaseClassTypeConverterSpec extends FlatSpec with Matchers {
  behavior of "CaseClassTypeConverter"

  import CaseClassTypeConverterSpec._

  it should "convert between exact classes" in {
    val converter =
      CaseClassTypeConverter[TransferObjectSame, InternalModelSame](identity)
    val transfer = TransferObjectSame(None, 0)
    val model = converter.convert(transfer)
    model should be(InternalModelSame(None, 0))
  }

  it should "convert between classes mapping fields when mapped correctly" in {
    val converter =
      CaseClassTypeConverter[TransferObjectDifferent, InternalModelDifferent] {
        _.filterKeys(_ != "toRemove")
          .updated("toAdd", 1.23)
          .map({
            case (fieldName, fieldValue) if fieldName == "oldB" =>
              "newB" -> fieldValue
            case other => other
          })
      }
    val transfer = TransferObjectDifferent(None, 0, toRemove = true)
    val model = converter.convert(transfer)
    model should be(InternalModelDifferent(None, 0, 1.23))
  }

  it should "fail to convert between incorrectly mapped classes" in {
    val converter =
      CaseClassTypeConverter[TransferObjectDifferent, InternalModelDifferent](
        identity
      )
    val transfer = TransferObjectDifferent(None, 0, toRemove = true)
    val exception = intercept[IllegalArgumentException] {
      converter.convert(transfer)
    }
    exception.getMessage should be(
      s"""|Unknown val for org.broadinstitute.clio.util.generic.CaseClassTypeConverterSpec$$InternalModelDifferent:
          |  oldB -> 0
          |  toRemove -> true""".stripMargin
    )
  }

}

object CaseClassTypeConverterSpec {

  case class TransferObjectSame(a: Option[String], b: Int)

  case class InternalModelSame(a: Option[String], b: Int)

  case class TransferObjectDifferent(a: Option[String], oldB: Int, toRemove: Boolean)

  case class InternalModelDifferent(a: Option[String], newB: Int, toAdd: Double)

}
