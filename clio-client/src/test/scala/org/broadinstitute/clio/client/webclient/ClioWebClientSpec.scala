package org.broadinstitute.clio.client.webclient

import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.util.MockClioServer

class ClioWebClientSpec extends BaseClientSpec {
  behavior of "ClioWebClient"

  val mockServer = new MockClioServer()

  override def beforeAll(): Unit = {
    super.beforeAll()
    mockServer.start()
  }

  override def afterAll(): Unit = {
    mockServer.stop()
    super.afterAll()
  }

  it should "time out requests that take too long" in {
    val client =
      new ClioWebClient("localhost", testServerPort, false, testRequestTimeout)

    recoverToSucceededIf[Exception] {
      client.getClioServerHealth
    }
  }
}
