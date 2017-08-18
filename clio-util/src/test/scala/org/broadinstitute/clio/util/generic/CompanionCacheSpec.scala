package org.broadinstitute.clio.util.generic

import org.scalatest.{FlatSpec, Matchers}

class CompanionCacheSpec extends FlatSpec with Matchers {
  behavior of "CompanionCache"

  it should "cache based on a companion type" in {
    import CompanionCacheSpec._
    val cache = new CompanionCache()
    val first = cache.cached[TestClass, TestClass.type, String] { _ =>
      "ok"
    }
    first should be("ok")

    val second = cache.cached[TestClass, TestClass.type, String] { _ =>
      throw new RuntimeException("Should have been cached.")
    }
    second should be("ok")
  }

  it should "fail to cache an inner class" in {
    sealed case class InnerClass(a: String)

    object InnerClass

    val cache = new CompanionCache()
    val exception = intercept[ClassNotFoundException] {
      cache.cached[InnerClass, InnerClass.type, String] { _ =>
        throw new RuntimeException("Should not have reached this point.")
      }
    }
    exception.getMessage should fullyMatch regex "org.broadinstitute.clio.util.generic.CompanionCacheSpec.InnerClass.*"
  }
}

object CompanionCacheSpec {

  sealed class TestClass()

  object TestClass

}
