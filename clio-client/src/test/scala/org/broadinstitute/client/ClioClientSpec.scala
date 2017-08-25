package org.broadinstitute.client

import caseapp.RemainingArgs
import org.broadinstitute.client.webclient.MockClioWebClient
import org.broadinstitute.clio.client.ClioClient

class ClioClientSpec extends SystemExitSpec {
  behavior of "ClioClient"

  it should "exit 1 if given a bad command" in {
    ClioClient.webClient = MockClioWebClient.returningInternalError
    ClioClient.beforeCommand(options = testCommon, remainingArgs = Seq.empty[String])
    val thrown = the [ExitException] thrownBy ClioClient.run(command = goodAddCommand, remainingArgs = RemainingArgs.apply(Seq.empty[String], Seq.empty[String]))
    thrown.status should be(1)
  }

  it should "exit 0 if the command is run successfully" in {
    ClioClient.webClient = MockClioWebClient.returningOk
    ClioClient.beforeCommand(options = testCommon, remainingArgs = Seq.empty[String])
    val thrown = the [ExitException] thrownBy ClioClient.run(command = goodAddCommand, remainingArgs = RemainingArgs.apply(Seq.empty[String], Seq.empty[String]))
    thrown.status should be(0)
  }
}
