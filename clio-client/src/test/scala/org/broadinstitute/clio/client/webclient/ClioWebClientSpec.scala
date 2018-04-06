package org.broadinstitute.clio.client.webclient

import java.util.concurrent.TimeoutException

import akka.stream.scaladsl.Sink
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport
import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.util.MockClioServer
import org.broadinstitute.clio.transfer.model.{ModelMockIndex, ModelMockQueryInput}
import org.broadinstitute.clio.util.json.ModelAutoDerivation

class ClioWebClientSpec
    extends BaseClientSpec
    with ModelAutoDerivation
    with ErrorAccumulatingCirceSupport {
  behavior of "ClioWebClient"

  val index = ModelMockIndex()
  val mockServer = new MockClioServer(index)

  override def beforeAll(): Unit = {
    super.beforeAll()
    mockServer.start()
  }

  override def afterAll(): Unit = {
    mockServer.stop()
    super.afterAll()
  }

  val client = new ClioWebClient(
    "localhost",
    testServerPort,
    false,
    testRequestTimeout,
    testMaxRetries,
    fakeTokenGenerator
  )

  it should "time out requests that take too long" in {
    recoverToSucceededIf[TimeoutException] {
      client.getClioServerHealth.runWith(Sink.ignore)
    }
  }

  it should "retry requests that fail with connection errors" in {
    // The mock clio-server is set up to timeout on requests to this route (maxRetries-1) times.
    client
      .query(index)(ModelMockQueryInput(), includeDeleted = false)
      .runWith(Sink.ignore)
      .map(_ => succeed)
  }
}
