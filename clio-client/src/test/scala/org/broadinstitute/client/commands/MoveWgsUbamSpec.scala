package org.broadinstitute.client.commands

import org.broadinstitute.client.BaseClientSpec
import org.broadinstitute.client.webclient.MockClioWebClient
import org.broadinstitute.clio.client.commands.{
  CommandDispatch,
  MoveWgsUbamCommand
}
import org.broadinstitute.clio.client.parser.BaseArgs
import org.broadinstitute.clio.client.util.IoUtil
import org.scalatest.BeforeAndAfterAll

class MoveWgsUbamSpec extends BaseClientSpec with BeforeAndAfterAll {
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
    IoUtil.copyGoogleObject(mockUbamPath, testUbamCloudSourcePath.get)
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
    IoUtil.copyGoogleObject(mockUbamPath, testUbamCloudSourcePath.get)
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
    IoUtil.copyGoogleObject(mockUbamPath, testUbamCloudSourcePath.get)
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

  override protected def afterAll(): Unit = {
    if (IoUtil.googleObjectExists(testUbamCloudSourcePath.get))
      IoUtil.deleteGoogleObject(testUbamCloudSourcePath.get)
    if (IoUtil.googleObjectExists(testUbamCloudDestinationPath.get))
      IoUtil.deleteGoogleObject(testUbamCloudDestinationPath.get)
    ()
  }
}
