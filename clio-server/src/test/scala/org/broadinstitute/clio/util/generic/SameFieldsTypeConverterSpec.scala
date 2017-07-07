package org.broadinstitute.clio.util.generic

import org.scalatest.{FlatSpec, Matchers}

class SameFieldsTypeConverterSpec extends FlatSpec with Matchers {
  behavior of "SameFieldsTypeConverter"

  it should "convert between classes" in {
    case class TransferObject(a: Option[String], b: Int)
    case class InternalModel(a: Option[String], b: Int)
    val converter = SameFieldsTypeConverter[TransferObject, InternalModel]
    val transfer = TransferObject(None, 0)
    val model = converter.convert(transfer)
    model should be(InternalModel(None, 0))
  }

}
