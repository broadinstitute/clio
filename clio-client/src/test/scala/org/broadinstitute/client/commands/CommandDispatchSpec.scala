package org.broadinstitute.client.commands

import org.broadinstitute.client.BaseClientSpec
import org.broadinstitute.client.webclient.MockClioWebClient
import org.broadinstitute.clio.client.commands.{CommandDispatch, Commands}
import org.broadinstitute.clio.client.parser.BaseArgs

class CommandDispatchSpec extends BaseClientSpec {
  behavior of "CommandDispatch"

  it should "return false when we dispatch a command that doesn't exist" in {
    CommandDispatch.dispatch(
      MockClioWebClient.returningOk,
      BaseArgs(command = None)
    ) should be(false)
  }

  it should "return true when we dispatch a valid queryReadGroup command" in {
    CommandDispatch.dispatch(
      MockClioWebClient.returningOk,
      BaseArgs(command = Some(Commands.QueryReadGroupBam.apply()))
    ) should be(true)
  }

  it should "return true when we dispatch a valid addReadGroup command" in {
    CommandDispatch.dispatch(
      MockClioWebClient.returningOk,
      BaseArgs(
        command = Some(Commands.AddReadGroupBam.apply()),
        flowcell = testFlowcell,
        lane = testLane,
        libraryName = testLibName,
        location = testLocation,
        bearerToken = testBearer,
        metadataLocation = metadataFileLocation
      )
    ) should be(true)
  }

}
