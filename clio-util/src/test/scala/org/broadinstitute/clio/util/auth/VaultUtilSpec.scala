package org.broadinstitute.clio.util.auth

import org.broadinstitute.clio.util.model.Env

import org.scalatest.{FlatSpec, Matchers}

class VaultUtilSpec extends FlatSpec with Matchers {
  behavior of "VaultUtil"

  // See http://www.scalatest.org/user_guide/sharing_tests
  Env.values.foreach {
    it should behave like credentialLoader(_)
  }
  def credentialLoader(env: Env): Unit = {
    val vaultUtil = new VaultUtil(env)

    it should s"be able to load credentials for env $env" in {
      noException should be thrownBy vaultUtil.credentialForScopes(Seq.empty)
    }
  }
}
