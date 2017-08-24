package org.broadinstitute.client

import org.broadinstitute.client.webclient.MockClioWebClient
import org.broadinstitute.clio.client.ClioClient

class ClioClientSpec extends BaseClientSpec {
  behavior of "ClioClient"

  val client: ClioClient = {
    val mockWebClient = MockClioWebClient.returningOk
    new ClioClient(mockWebClient, goodAddCommand, testBearer)
  }

  val badClient: ClioClient = {
    val mockClioWebClient = MockClioWebClient.returningInternalError
    new ClioClient(mockClioWebClient, goodAddCommand, testBearer)
  }

  it should "exit 1 if given a bad command" in {
    badClient.execute() should be(1)
  }

  it should "exit 0 if the command is run successfully" in {
    client.execute() should be(0)
  }
}
