package org.broadinstitute.client.commands

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.client.BaseClientSpec
import org.broadinstitute.client.util.MockIoUtil
import org.broadinstitute.client.webclient.MockClioWebClient
import org.broadinstitute.clio.client.commands.CommandDispatch
import org.broadinstitute.clio.client.commands.Commands.DeleteWgsUbam
import org.broadinstitute.clio.client.parser.BaseArgs

class DeleteWgsUbamSpec extends BaseClientSpec {
  behavior of "DeleteWgsUbam"

  it should "throw an exception if the location is not GCP" in {
    recoverToSucceededIf[Exception] {
      val config = BaseArgs(
        command = Some(DeleteWgsUbam),
        flowcell = testFlowcell,
        lane = testLane,
        libraryName = testLibName,
        location = Some("OnPrem"),
        bearerToken = testBearer,
      )
      succeedingDispatcher.dispatch(config)
    }
  }

  it should "throw an exception if Clio returns an error" in {
    recoverToSucceededIf[Exception] {
      val config = BaseArgs(
        command = Some(DeleteWgsUbam),
        flowcell = testFlowcell,
        lane = testLane,
        libraryName = testLibName,
        location = testLocation,
        bearerToken = testBearer,
      )
      failingDispatcher.dispatch(config)
    }
  }

  it should "throw an exception if Clio doesn't return a Ubam" in {
    recoverToSucceededIf[Exception] {
      val config = BaseArgs(
        command = Some(DeleteWgsUbam),
        flowcell = testFlowcell,
        lane = testLane,
        libraryName = testLibName,
        location = testLocation,
        bearerToken = testBearer,
      )
      succeedingDispatcher.dispatch(config)
    }
  }

  it should "throw an exception if Clio can't delete the cloud file" in {
    class FailingDeleteMockIoUtil extends MockIoUtil {
      override def deleteGoogleObject(path: String): Int = 1
      override def googleObjectExists(path: String): Boolean = true
    }
    recoverToSucceededIf[Exception] {
      val config = BaseArgs(
        command = Some(DeleteWgsUbam),
        flowcell = testFlowcell,
        lane = testLane,
        libraryName = testLibName,
        location = testLocation,
        bearerToken = testBearer,
      )
      succeedingReturningDispatcher(new FailingDeleteMockIoUtil)
        .dispatch(config)
    }
  }

  it should "throw an exception if Clio can't delete the WgsUbam" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testUbamCloudSourcePath.get)
    recoverToSucceededIf[Exception] {
      val config = BaseArgs(
        command = Some(DeleteWgsUbam),
        flowcell = testFlowcell,
        lane = testLane,
        libraryName = testLibName,
        location = testLocation,
        bearerToken = testBearer,
      )
      new CommandDispatch(MockClioWebClient.failingToAddWgsUbam, mockIoUtil)
        .dispatch(config)
    }
  }

  it should "delete a WgsUbam in Clio if the cloud ubam does not exist" in {
    val config = BaseArgs(
      command = Some(DeleteWgsUbam),
      flowcell = testFlowcell,
      lane = testLane,
      libraryName = testLibName,
      location = testLocation,
      bearerToken = testBearer,
    )
    succeedingReturningDispatcher(new MockIoUtil)
      .dispatch(config)
      .map(_.status should be(StatusCodes.OK))
  }

  it should "delete a WgsUbam in Clio and the cloud" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testUbamCloudSourcePath.get)
    val config = BaseArgs(
      command = Some(DeleteWgsUbam),
      flowcell = testFlowcell,
      lane = testLane,
      libraryName = testLibName,
      location = testLocation,
      bearerToken = testBearer,
    )
    succeedingReturningDispatcher(mockIoUtil)
      .dispatch(config)
      .map(_.status should be(StatusCodes.OK))
  }

  it should "delete multiple WgsUbams in Clio and the cloud" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testUbamCloudSourcePath.get)
    mockIoUtil.putFileInCloud(testUbamCloudDestinationPath.get)
    val config = BaseArgs(
      command = Some(DeleteWgsUbam),
      flowcell = testFlowcell,
      libraryName = testLibName,
      location = testLocation,
      bearerToken = testBearer,
    )
    new CommandDispatch(MockClioWebClient.returningTwoWgsUbams, mockIoUtil)
      .dispatch(config)
      .map(_.status should be(StatusCodes.OK))
  }
}
