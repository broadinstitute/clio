package org.broadinstitute.client

import org.broadinstitute.client.util.MockIoUtil
import org.broadinstitute.client.webclient.MockClioWebClient
import org.broadinstitute.clio.client.ClioClient
import org.broadinstitute.clio.client.commands.CommandDispatch

import scala.concurrent.ExecutionContext

class ClioClientSpec extends BaseClientSpec {
  behavior of "ClioClient"

  override implicit val executionContext: ExecutionContext = system.dispatcher

  val client: ClioClient = {
    val mockWebClient = MockClioWebClient.returningOk
    val commandDispatch = new CommandDispatch(mockWebClient, MockIoUtil)
    new ClioClient(commandDispatch)
  }

  it should "exit 1 if given a bad command" in {
    recoverToSucceededIf[Exception] {
      client.execute(badCommand)
    }
  }

  it should "exit 0 if the command is run successfully" in {
    client.execute(goodAddCommand).map(_ should be(()))
  }
}
