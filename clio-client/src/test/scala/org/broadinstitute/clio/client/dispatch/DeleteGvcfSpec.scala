package org.broadinstitute.clio.client.dispatch

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.commands.DeleteGvcf
import org.broadinstitute.clio.client.util.MockIoUtil
import org.broadinstitute.clio.client.webclient.MockClioWebClient
import org.broadinstitute.clio.transfer.model.gvcf.TransferGvcfV1Key
import org.broadinstitute.clio.util.model.Location

class DeleteGvcfSpec extends BaseClientSpec {
  behavior of "DeleteGvcf"

  implicit val bearToken: OAuth2BearerToken = testBearer

  it should "throw an exception if the location is not GCP" in {
    recoverToSucceededIf[Exception] {
      val command = DeleteGvcf(
        transferGvcfV1Key = TransferGvcfV1Key(
          location = Location.OnPrem,
          project = testProject,
          sampleAlias = testSampleAlias,
          version = testVersion
        ),
        note = "Deleting for test"
      )
      succeedingDispatcher.dispatch(command)
    }
  }

  it should "throw an exception if Clio returns an error" in {
    recoverToSucceededIf[Exception] {
      failingDispatcher.dispatch(goodGvcfDeleteCommand)
    }
  }

  it should "throw an exception if Clio doesn't return a gvcf" in {
    recoverToSucceededIf[Exception] {
      succeedingDispatcher.dispatch(goodGvcfDeleteCommand)
    }
  }

  it should "throw an exception if Clio can't delete the cloud file" in {
    class FailingDeleteMockIoUtil extends MockIoUtil {
      override def deleteGoogleObject(path: String): Int = 1
      override def googleObjectExists(path: String): Boolean = true
    }
    recoverToSucceededIf[Exception] {
      succeedingReturningDispatcherGvcf(new FailingDeleteMockIoUtil)
        .dispatch(goodGvcfDeleteCommand)
    }
  }

  it should "throw an exception if Clio can't delete the gvcf" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testGvcfCloudSourcePath)
    recoverToSucceededIf[Exception] {
      new CommandDispatch(MockClioWebClient.failingToUpsert, mockIoUtil)
        .dispatch(goodGvcfDeleteCommand)
    }
  }

  it should "delete a gvcf in Clio if the cloud gvcf does not exist" in {
    succeedingReturningDispatcherGvcf(new MockIoUtil)
      .dispatch(goodGvcfDeleteCommand)
      .map(_.status should be(StatusCodes.OK))
  }

  it should "delete a gvcf in Clio and the cloud" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testGvcfCloudSourcePath)
    succeedingReturningDispatcherGvcf(mockIoUtil)
      .dispatch(goodGvcfDeleteCommand)
      .map(_.status should be(StatusCodes.OK))
  }

  it should "delete multiple gvcfs in Clio and the cloud" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testGvcfCloudSourcePath)
    mockIoUtil.putFileInCloud(testGvcfCloudDestinationDirectoryPath)

    new CommandDispatch(MockClioWebClient.returningTwoGvcfs, mockIoUtil)
      .dispatch(goodGvcfDeleteCommand)
      .map(_.status should be(StatusCodes.OK))
  }
}
