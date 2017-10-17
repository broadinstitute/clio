package org.broadinstitute.clio.util.generic

import org.scalatest.{FlatSpec, Matchers}
import s_mach.string._

class CirceEquivalentCamelCaseLexerSpec extends FlatSpec with Matchers {
  behavior of "CirceEquivalentCamelCaseLexerSpec"

  it should "break camel-case at the last uppercase letter in a sequence of uppercase letters" in {
    val fieldName = "preBqsrSelfSMPath"
    fieldName.toSnakeCase(CirceEquivalentCamelCaseLexer) should be(
      "pre_bqsr_self_sm_path"
    )
  }

  it should "break camel-case where a number is followed by an uppercase letter" in {
    val fieldName = "cramMd5Path"
    fieldName.toSnakeCase(CirceEquivalentCamelCaseLexer) should be(
      "cram_md5_path"
    )
  }
}
