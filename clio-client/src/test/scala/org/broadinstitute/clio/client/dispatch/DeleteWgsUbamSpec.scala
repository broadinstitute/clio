package org.broadinstitute.clio.client.dispatch

import java.net.URI

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.commands.DeleteWgsUbam
import org.broadinstitute.clio.client.util.MockIoUtil
import org.broadinstitute.clio.client.webclient.MockClioWebClient
import org.broadinstitute.clio.transfer.model.ubam.TransferUbamV1Key
import org.broadinstitute.clio.util.model.{Location, UpsertId}

class DeleteWgsUbamSpec extends BaseClientSpec {
  behavior of "DeleteWgsUbam"

  implicit val bearToken: OAuth2BearerToken = testBearer

  it should "throw an exception if the location is not GCP" in {
    recoverToSucceededIf[UnsupportedOperationException] {
      val command = DeleteWgsUbam(
        key = TransferUbamV1Key(
          flowcellBarcode = testFlowcell,
          lane = testLane,
          libraryName = testLibName,
          location = Location.OnPrem
        ),
        note = "Deleting for test"
      )
      succeedingDispatcher().dispatch(command)
    }
  }

  it should "throw an exception if Clio returns an error" in {
    recoverToSucceededIf[RuntimeException] {
      failingDispatcher.dispatch(goodDeleteCommand)
    }
  }

  it should "throw an exception if Clio doesn't return a ubam" in {
    recoverToSucceededIf[IllegalStateException] {
      succeedingDispatcher().dispatch(goodDeleteCommand)
    }
  }

  it should "throw an exception if Clio returns multiple records for a ubam" in {
    val mockIoUtil = new MockIoUtil
    recoverToSucceededIf[IllegalStateException] {
      succeedingDispatcher(mockIoUtil, testTwoWgsUbamsLocation)
        .dispatch(goodDeleteCommand)
    }
  }

  it should "throw an exception if Clio can't delete the cloud file" in {
    recoverToSucceededIf[RuntimeException] {
      succeedingDispatcher(
        new MockIoUtil {
          override def deleteGoogleObject(path: URI): Int = 1
          override def googleObjectExists(path: URI): Boolean = true
        },
        testWgsUbamLocation
      ).dispatch(goodDeleteCommand)
    }
  }

  it should "throw an exception if Clio can't delete the ubam record" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testUbamCloudSourcePath)
    recoverToSucceededIf[RuntimeException] {
      new CommandDispatch(MockClioWebClient.failingToUpsert, mockIoUtil)
        .dispatch(goodDeleteCommand)
    }
  }

  it should "throw an exception when attempting to delete a ubam in Clio if the cloud ubam does not exist" in {
    recoverToSucceededIf[IllegalStateException] {
      succeedingDispatcher(new MockIoUtil, testWgsUbamLocation)
        .dispatch(goodDeleteCommand)
    }
  }

  it should "delete a ubam in Clio if the cloud ubam does not exist and the force flag is true" in {
    succeedingDispatcher(new MockIoUtil, testWgsUbamLocation)
      .dispatch(goodDeleteCommand.copy(force = true))
      .map(_ shouldBe an[UpsertId])
  }

  it should "delete a ubam in Clio and the cloud" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testUbamCloudSourcePath)
    succeedingDispatcher(mockIoUtil, testWgsUbamLocation)
      .dispatch(goodDeleteCommand)
      .map(_ shouldBe an[UpsertId])
  }
}
