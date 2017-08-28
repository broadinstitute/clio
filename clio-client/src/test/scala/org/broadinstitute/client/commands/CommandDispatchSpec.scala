package org.broadinstitute.client.commands

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.client.BaseClientSpec
import org.broadinstitute.client.util.MockIoUtil
import org.broadinstitute.clio.client.commands.Commands
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
    succeedingDispatcherCamel
      .dispatch(BaseArgs(command = Some(Commands.QueryWgsUbam)))
      .map(_.status should be(StatusCodes.OK))
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
      .map(_.status should be(StatusCodes.OK))
  }

  it should "return true when we dispatch a valid moveWgsUbam command" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testUbamCloudSourcePath.get)
    val config = BaseArgs(
      command = Some(Commands.MoveWgsUbam),
      flowcell = testFlowcell,
      lane = testLane,
      libraryName = testLibName,
      location = testLocation,
      bearerToken = testBearer,
      ubamPath = testUbamCloudDestinationPath
    )
    succeedingReturningDispatcher(mockIoUtil)
      .dispatch(config)
      .map(_.status should be(StatusCodes.OK))
  }
}
