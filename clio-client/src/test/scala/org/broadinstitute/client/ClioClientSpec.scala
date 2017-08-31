package org.broadinstitute.client

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.client.util.MockIoUtil
import org.broadinstitute.client.webclient.MockClioWebClient
import org.broadinstitute.clio.client.ClioClient
import org.broadinstitute.clio.client.commands.CommandDispatch

import scala.concurrent.ExecutionContext

class ClioClientSpec extends SystemExitSpec {
  behavior of "ClioClient"

  override implicit val executionContext: ExecutionContext = system.dispatcher
  implicit val bearerToken: OAuth2BearerToken = testBearer

  val client: ClioClient = {
    val mockWebClient = MockClioWebClient.returningOk
    val commandDispatch = new CommandDispatch(mockWebClient, new MockIoUtil)
    new ClioClient(commandDispatch)
  }

  it should "exit 255 if given a bad command" in {
    val thrown = the[ExitException] thrownBy ClioClient.main(
      Array("badCommand")
    )
    thrown.status should be(255)
  }

  it should "exit 0 if the command is run successfully" in {
    client
      .execute(goodAddCommand)
      .map(r => r.status should be(StatusCodes.OK))
  }
}
