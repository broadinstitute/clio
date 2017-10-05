package org.broadinstitute.clio.client.dispatch

import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.commands.DeleteWgsUbam
import org.broadinstitute.clio.client.util.MockIoUtil
import org.broadinstitute.clio.client.webclient.MockClioWebClient
import org.broadinstitute.clio.util.model.Location
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.clio.transfer.model.wgsubam.TransferWgsUbamV1Key

class DeleteWgsUbamSpec extends BaseClientSpec {
  behavior of "DeleteWgsUbam"

  implicit val bearToken: OAuth2BearerToken = testBearer

  it should "throw an exception if the location is not GCP" in {
    recoverToSucceededIf[Exception] {
      val command = DeleteWgsUbam(
        key = TransferWgsUbamV1Key(
          flowcellBarcode = testFlowcell,
          lane = testLane,
          libraryName = testLibName,
          location = Location.OnPrem
        ),
        note = "Deleting for test"
      )
      succeedingDispatcher.dispatch(command)
    }
  }

  it should "throw an exception if Clio returns an error" in {
    recoverToSucceededIf[Exception] {
      failingDispatcher.dispatch(goodDeleteCommand)
    }
  }

  it should "throw an exception if Clio doesn't return a WgsUbam" in {
    recoverToSucceededIf[Exception] {
      succeedingDispatcher.dispatch(goodDeleteCommand)
    }
  }

  it should "throw an exception if Clio returns multiple records for a WgsUbam" in {
    val mockIoUtil = new MockIoUtil
    recoverToSucceededIf[Exception] {
      new CommandDispatch(MockClioWebClient.returningTwoWgsUbams, mockIoUtil)
        .dispatch(goodDeleteCommand)
    }
  }

  it should "throw an exception if Clio can't delete the cloud file" in {
    class FailingDeleteMockIoUtil extends MockIoUtil {
      override def deleteGoogleObject(path: String): Int = 1
      override def googleObjectExists(path: String): Boolean = true
    }
    recoverToSucceededIf[Exception] {
      succeedingReturningDispatcherWgsUbam(new FailingDeleteMockIoUtil)
        .dispatch(goodDeleteCommand)
    }
  }

  it should "throw an exception if Clio can't delete the WgsUbam" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testUbamCloudSourcePath)
    recoverToSucceededIf[Exception] {
      new CommandDispatch(MockClioWebClient.failingToUpsert, mockIoUtil)
        .dispatch(goodDeleteCommand)
    }
  }

  it should "delete a WgsUbam in Clio if the cloud ubam does not exist" in {
    succeedingReturningDispatcherWgsUbam(new MockIoUtil)
      .dispatch(goodDeleteCommand)
      .map(_.status should be(StatusCodes.OK))
  }

  it should "delete a WgsUbam in Clio and the cloud" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testUbamCloudSourcePath)
    succeedingReturningDispatcherWgsUbam(mockIoUtil)
      .dispatch(goodDeleteCommand)
      .map(_.status should be(StatusCodes.OK))
  }
}
