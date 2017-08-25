package org.broadinstitute.client.commands

import org.broadinstitute.client.BaseClientSpec
import org.broadinstitute.client.util.MockIoUtil
import org.broadinstitute.client.webclient.MockClioWebClient
import org.broadinstitute.clio.client.commands.{
  CommandDispatch,
  MoveWgsUbamCommand
}
import org.broadinstitute.clio.client.parser.BaseArgs

class MoveWgsUbamSpec extends BaseClientSpec {
  behavior of "MoveWgsUbam"

  it should "throw an exception if the destination path scheme is invalid" in {
    recoverToSucceededIf[Exception] {
      val config = BaseArgs(
        flowcell = testFlowcell,
        lane = testLane,
        libraryName = testLibName,
        location = testLocation,
        bearerToken = testBearer,
        ubamPath = Some("error://not_a_valid_path")
      )
      CommandDispatch.checkResponse(
        MoveWgsUbamCommand.execute(MockClioWebClient.returningOk, config)
      )
    }
  }

  it should "throw an exception if the unmapped bam file does not exist" in {
    recoverToSucceededIf[Exception] {
      val config = BaseArgs(
        flowcell = testFlowcell,
        lane = testLane,
        libraryName = testLibName,
        location = testLocation,
        bearerToken = testBearer,
        ubamPath = testUbamCloudSourcePath
      )
      CommandDispatch.checkResponse(
        MoveWgsUbamCommand.execute(MockClioWebClient.returningOk, config)
      )
    }
  }

  it should "throw an exception if the source and destination paths are the same" in {
    MockIoUtil.deleteAllInCloud()
    MockIoUtil.putFileInCloud(testUbamCloudSourcePath.get)
    recoverToSucceededIf[Exception] {
      val config = BaseArgs(
        flowcell = testFlowcell,
        lane = testLane,
        libraryName = testLibName,
        location = testLocation,
        bearerToken = testBearer,
        ubamPath = testUbamCloudSourcePath
      )
      CommandDispatch.checkResponse(
        MoveWgsUbamCommand.execute(MockClioWebClient.returningOk, config)
      )
    }
  }

  it should "throw an exception if Clio returns an error" in {
    recoverToSucceededIf[Exception] {
      val config = BaseArgs(
        flowcell = testFlowcell,
        lane = testLane,
        libraryName = testLibName,
        location = testLocation,
        bearerToken = testBearer,
        ubamPath = testUbamCloudSourcePath
      )
      CommandDispatch.checkResponse(
        MoveWgsUbamCommand
          .execute(MockClioWebClient.returningInternalError, config)
      )
    }
  }

  it should "throw an exception if Clio doesn't return a Ubam" in {
    recoverToSucceededIf[Exception] {
      val config = BaseArgs(
        flowcell = testFlowcell,
        lane = testLane,
        libraryName = testLibName,
        location = testLocation,
        bearerToken = testBearer,
        ubamPath = testUbamCloudSourcePath
      )
      CommandDispatch.checkResponse(
        MoveWgsUbamCommand
          .execute(MockClioWebClient.returningNoWgsUbam, config)
      )
    }
  }

  it should "throw an exception if Clio can't upsert the new WgsUbam" in {
    MockIoUtil.deleteAllInCloud()
    MockIoUtil.putFileInCloud(testUbamCloudSourcePath.get)
    recoverToSucceededIf[Exception] {
      val config = BaseArgs(
        flowcell = testFlowcell,
        lane = testLane,
        libraryName = testLibName,
        location = testLocation,
        bearerToken = testBearer,
        ubamPath = testUbamCloudDestinationPath
      )
      CommandDispatch.checkResponse(
        MoveWgsUbamCommand
          .execute(MockClioWebClient.failingToAddWgsUbam, config)
      )
    }
  }

  it should "move clio unmapped bams if no errors are encountered" in {
    MockIoUtil.deleteAllInCloud()
    MockIoUtil.putFileInCloud(testUbamCloudSourcePath.get)
    val config = BaseArgs(
      flowcell = testFlowcell,
      lane = testLane,
      libraryName = testLibName,
      location = testLocation,
      bearerToken = testBearer,
      ubamPath = testUbamCloudDestinationPath
    )
    CommandDispatch
      .checkResponse(
        MoveWgsUbamCommand
          .execute(MockClioWebClient.returningOk, config)
      )
      .map(_ should be(true))
  }
}
