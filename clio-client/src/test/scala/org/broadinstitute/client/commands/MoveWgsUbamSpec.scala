package org.broadinstitute.client.commands

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.client.BaseClientSpec
import org.broadinstitute.client.util.MockIoUtil
import org.broadinstitute.client.webclient.MockClioWebClient
import org.broadinstitute.clio.client.commands.CommandDispatch
import org.broadinstitute.clio.client.commands.Commands.MoveWgsUbam
import org.broadinstitute.clio.client.parser.BaseArgs

class MoveWgsUbamSpec extends BaseClientSpec {

  behavior of "MoveWgsUbam"

  it should "throw an exception if the destination path scheme is invalid" in {
    recoverToSucceededIf[Exception] {
      val config = BaseArgs(
        command = Some(MoveWgsUbam),
        flowcell = testFlowcell,
        lane = testLane,
        libraryName = testLibName,
        location = testLocation,
        bearerToken = testBearer,
        ubamPath = Some("gs://not_a_valid_path")
      )
      succeedingDispatcher.dispatch(config)
    }
  }

  it should "throw an exception if the unmapped bam file does not exist" in {
    recoverToSucceededIf[Exception] {
      val config = BaseArgs(
        command = Some(MoveWgsUbam),
        flowcell = testFlowcell,
        lane = testLane,
        libraryName = testLibName,
        location = testLocation,
        bearerToken = testBearer,
        ubamPath = testUbamCloudSourcePath
      )
      succeedingDispatcher.dispatch(config)
    }
  }

  it should "throw an exception if the source and destination paths are the same" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testUbamCloudSourcePath.get)
    recoverToSucceededIf[Exception] {
      val config = BaseArgs(
        command = Some(MoveWgsUbam),
        flowcell = testFlowcell,
        lane = testLane,
        libraryName = testLibName,
        location = testLocation,
        bearerToken = testBearer,
        ubamPath = testUbamCloudSourcePath
      )
      succeedingReturningDispatcher(mockIoUtil).dispatch(config)
    }
  }

  it should "throw an exception if Clio returns an error" in {
    recoverToSucceededIf[Exception] {
      val config = BaseArgs(
        command = Some(MoveWgsUbam),
        flowcell = testFlowcell,
        lane = testLane,
        libraryName = testLibName,
        location = testLocation,
        bearerToken = testBearer,
        ubamPath = testUbamCloudSourcePath
      )
      succeedingDispatcher.dispatch(config)
    }
  }

  it should "throw an exception if Clio doesn't return a Ubam" in {
    recoverToSucceededIf[Exception] {
      val config = BaseArgs(
        command = Some(MoveWgsUbam),
        flowcell = testFlowcell,
        lane = testLane,
        libraryName = testLibName,
        location = testLocation,
        bearerToken = testBearer,
        ubamPath = testUbamCloudSourcePath
      )
      succeedingDispatcher.dispatch(config)
    }
  }

  it should "throw an exception if Clio can't upsert the new WgsUbam" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testUbamCloudSourcePath.get)
    recoverToSucceededIf[Exception] {
      val config = BaseArgs(
        command = Some(MoveWgsUbam),
        flowcell = testFlowcell,
        lane = testLane,
        libraryName = testLibName,
        location = testLocation,
        bearerToken = testBearer,
        ubamPath = testUbamCloudDestinationPath
      )
      new CommandDispatch(MockClioWebClient.failingToAddWgsUbam, mockIoUtil)
        .dispatch(config)
    }
  }

  it should "throw and exception if given a non-GCP unmapped bam" in {
    recoverToSucceededIf[Exception] {
      val config = BaseArgs(
        command = Some(MoveWgsUbam),
        flowcell = testFlowcell,
        lane = testLane,
        libraryName = testLibName,
        location = Some("OnPrem"),
        bearerToken = testBearer,
        ubamPath = testUbamCloudDestinationPath
      )
      succeedingDispatcher.dispatch(config)
    }
  }

  it should "throw and exception if the destination path is not in GCP" in {
    recoverToSucceededIf[Exception] {
      val config = BaseArgs(
        command = Some(MoveWgsUbam),
        flowcell = testFlowcell,
        lane = testLane,
        libraryName = testLibName,
        location = testLocation,
        bearerToken = testBearer,
        ubamPath = Some("/this/is/a/local/path")
      )
      succeedingDispatcher.dispatch(config)
    }
  }

  it should "move clio unmapped bams if no errors are encountered" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testUbamCloudSourcePath.get)
    val config = BaseArgs(
      command = Some(MoveWgsUbam),
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
