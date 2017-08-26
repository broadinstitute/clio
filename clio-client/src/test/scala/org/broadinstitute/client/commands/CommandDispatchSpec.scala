package org.broadinstitute.client.commands

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.client.BaseClientSpec
import org.broadinstitute.client.util.MockIoUtil
import org.broadinstitute.client.webclient.MockClioWebClient
import org.broadinstitute.clio.client.commands.{CommandDispatch, Commands}
import org.broadinstitute.clio.client.parser.BaseArgs

class CommandDispatchSpec extends BaseClientSpec {
  behavior of "CommandDispatch"

  it should "return false when we dispatch a command that doesn't exist" in {
    recoverToSucceededIf[Exception] {
      succeedingDispatcher
        .dispatch(BaseArgs(command = None))
    }
  }

  it should "return true when we dispatch a valid queryWgsUbam command" in {
    succeedingDispatcher
      .dispatch(BaseArgs(command = Some(Commands.QueryWgsUbam)))
      .map(_ should be(()))
  }

  it should "return true when we dispatch a valid addWgsUbam command" in {
    succeedingDispatcher
      .dispatch(
        BaseArgs(
          command = Some(Commands.AddWgsUbam),
          flowcell = testFlowcell,
          lane = testLane,
          libraryName = testLibName,
          location = testLocation,
          bearerToken = testBearer,
          metadataLocation = metadataFileLocation
        )
      )
      .map(_ should be(()))
  }

  it should "return true when we dispatch a valid moveWgsUbam command" in {
    MockIoUtil.resetMockState()
    MockIoUtil.putFileInCloud(testUbamCloudSourcePath.get)
    val config = BaseArgs(
      command = Some(Commands.MoveWgsUbam),
      flowcell = testFlowcell,
      lane = testLane,
      libraryName = testLibName,
      location = testLocation,
      bearerToken = testBearer,
      ubamPath = testUbamCloudDestinationPath
    )
    val dispatcher = new CommandDispatch(
      new MockClioWebClient(StatusCodes.OK, snakeCaseMetadataFileLocation.get),
      MockIoUtil
    )
    dispatcher.dispatch(config).map(_ should be(()))
  }
}
