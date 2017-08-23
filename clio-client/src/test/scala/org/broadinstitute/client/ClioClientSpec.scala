package org.broadinstitute.client

import org.broadinstitute.client.webclient.MockClioWebClient
import org.broadinstitute.clio.client.ClioClient

class ClioClientSpec extends BaseClientSpec {
  behavior of "ClioClient"

  val client: ClioClient = {
    val mockWebClient = MockClioWebClient.returningOk
    new ClioClient(mockWebClient)
  }

  it should "exit 1 if given a bad command" in {
    client.execute(badCommand) should be(1)
  }

  it should "exit 0 if the command is run successfully" in {
    client.execute(goodAddCommand) should be(0)
  }
}