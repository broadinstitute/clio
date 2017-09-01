package org.broadinstitute.client.commands

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.client.BaseClientSpec
import org.broadinstitute.client.util.MockIoUtil
import org.broadinstitute.client.webclient.MockClioWebClient
import org.broadinstitute.clio.client.commands.{CommandDispatch, DeleteWgsUbam}
import org.broadinstitute.clio.transfer.model.{
  TransferWgsUbamV1Key,
  TransferWgsUbamV1Metadata
}
import org.broadinstitute.clio.util.model.Location

class DeleteWgsUbamSpec extends BaseClientSpec {
  behavior of "DeleteWgsUbam"

  implicit val bearToken: OAuth2BearerToken = testBearer

  it should "throw an exception if the location is not GCP" in {
    recoverToSucceededIf[Exception] {
      val command = DeleteWgsUbam(
        metadata =
          TransferWgsUbamV1Metadata(ubamPath = testUbamCloudDestinationPath),
        transferWgsUbamV1Key = TransferWgsUbamV1Key(
          flowcellBarcode = testFlowcell,
          lane = testLane,
          libraryName = testLibName,
          location = Location.OnPrem
        )
      )
      succeedingDispatcher.dispatch(command)
    }
  }

  it should "throw an exception if Clio returns an error" in {
    recoverToSucceededIf[Exception] {
      failingDispatcher.dispatch(goodDeleteCommand)
    }
  }

  it should "throw an exception if Clio doesn't return a Ubam" in {
    recoverToSucceededIf[Exception] {
      succeedingDispatcher.dispatch(goodDeleteCommand)
    }
  }

  it should "throw an exception if Clio can't delete the cloud file" in {
    class FailingDeleteMockIoUtil extends MockIoUtil {
      override def deleteGoogleObject(path: String): Int = 1
      override def googleObjectExists(path: String): Boolean = true
    }
    recoverToSucceededIf[Exception] {
      succeedingReturningDispatcher(new FailingDeleteMockIoUtil)
        .dispatch(goodDeleteCommand)
    }
  }

  it should "throw an exception if Clio can't delete the WgsUbam" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testUbamCloudSourcePath.get)
    recoverToSucceededIf[Exception] {
      new CommandDispatch(MockClioWebClient.failingToAddWgsUbam, mockIoUtil)
        .dispatch(goodDeleteCommand)
    }
  }

  it should "delete a WgsUbam in Clio if the cloud ubam does not exist" in {
    succeedingReturningDispatcher(new MockIoUtil)
      .dispatch(goodDeleteCommand)
      .map(_.status should be(StatusCodes.OK))
  }

  it should "delete a WgsUbam in Clio and the cloud" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testUbamCloudSourcePath.get)
    succeedingReturningDispatcher(mockIoUtil)
      .dispatch(goodDeleteCommand)
      .map(_.status should be(StatusCodes.OK))
  }

  it should "delete multiple WgsUbams in Clio and the cloud" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testUbamCloudSourcePath.get)
    mockIoUtil.putFileInCloud(testUbamCloudDestinationPath.get)

    new CommandDispatch(MockClioWebClient.returningTwoWgsUbams, mockIoUtil)
      .dispatch(goodDeleteCommand)
      .map(_.status should be(StatusCodes.OK))
  }
}
