package org.broadinstitute.client

import org.broadinstitute.client.util.TestData
import org.scalatest.{AsyncFlatSpec, Matchers}

class ClioClientSpec extends AsyncFlatSpec with TestData with Matchers {
  behavior of "ClioClient"

  it should "exit 1 if given a bad command" in {
    MockClioClient().execute(badCommand) should be(1)
  }

  it should "exit 0 if the command is run successfully" in {
    MockClioClient().execute(goodAddCommand) should be(0)
  }
}
