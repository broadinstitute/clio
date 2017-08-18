package org.broadinstitute.client.parser

import org.broadinstitute.client.util.TestData
import org.broadinstitute.clio.client.parser.{BaseArgs, BaseParser}
import org.scalatest.{AsyncFlatSpec, Matchers}

class BaseArgsSpec extends AsyncFlatSpec with Matchers with TestData {
  behavior of "BaseArgs"

  val parser = new BaseParser

  it should "error out if you don't supply a command" in {
    parser.parse(noCommand, BaseArgs()) shouldBe empty
  }

  it should "error if you don't provide a required argument" in {
    parser.parse(missingRequired, BaseArgs()) shouldBe empty
  }

  it should "parse configuration successfully with a missing optional argument" in {
    parser.parse(missingOptional, BaseArgs()) shouldBe defined
  }

  it should "parse configuration successfully with a good command" in {
    parser.parse(goodAddCommand, BaseArgs()) shouldBe defined
  }
}
