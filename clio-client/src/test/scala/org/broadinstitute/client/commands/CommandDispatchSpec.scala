package org.broadinstitute.client.commands

import org.broadinstitute.client.BaseClientSpec
import org.broadinstitute.client.util.MockIoUtil
import org.broadinstitute.clio.client.commands.Commands
import org.broadinstitute.clio.client.parser.BaseArgs

class CommandDispatchSpec extends BaseClientSpec {
  behavior of "CommandDispatch"

  it should "return false when we dispatch a command that doesn't exist" in {
    succeedingDispatcher
      .dispatch(BaseArgs(command = None))
      .map(_ should be(false))
  }

  it should "return true when we dispatch a valid queryWgsUbam command" in {
    succeedingDispatcher
      .dispatch(BaseArgs(command = Some(Commands.QueryWgsUbam)))
      .map(_ should be(true))
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
      .map(_ should be(true))
  }

  it should "return true when we dispatch a valid moveWgsUbam command" in {
    MockIoUtil.deleteAllInCloud()
    MockIoUtil.putFileInCloud(testUbamCloudSourcePath.get)
    succeedingDispatcher
      .dispatch(
        BaseArgs(
          command = Some(Commands.MoveWgsUbam),
          flowcell = testFlowcell,
          lane = testLane,
          libraryName = testLibName,
          location = testLocation,
          bearerToken = testBearer,
          ubamPath = testUbamCloudDestinationPath
        )
      )
      .map(_ should be(true))
  }

}
