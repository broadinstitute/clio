package org.broadinstitute.clio.client.dispatch

import java.net.URI

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.commands.DeleteGvcf
import org.broadinstitute.clio.client.util.MockIoUtil
import org.broadinstitute.clio.client.webclient.MockClioWebClient
import org.broadinstitute.clio.transfer.model.gvcf.TransferGvcfV1Key
import org.broadinstitute.clio.util.model.{Location, UpsertId}

class DeleteGvcfSpec extends BaseClientSpec {
  behavior of "DeleteGvcf"

  implicit val bearToken: OAuth2BearerToken = testBearer

  it should "throw an exception if the location is not GCP" in {
    recoverToSucceededIf[UnsupportedOperationException] {
      val command = DeleteGvcf(
        key = TransferGvcfV1Key(
          location = Location.OnPrem,
          project = testProject,
          sampleAlias = testSampleAlias,
          version = testVersion
        ),
        note = "Deleting for gvcf test"
      )
      succeedingDispatcher().dispatch(command)
    }
  }

  it should "throw an exception if Clio returns an error" in {
    recoverToSucceededIf[RuntimeException] {
      failingDispatcher.dispatch(goodGvcfDeleteCommand)
    }
  }

  it should "throw an exception if Clio doesn't return a gvcf" in {
    recoverToSucceededIf[IllegalStateException] {
      succeedingDispatcher().dispatch(goodGvcfDeleteCommand)
    }
  }

  it should "throw an exception if Clio returns multiple records for a gvcf" in {
    val mockIoUtil = new MockIoUtil
    recoverToSucceededIf[IllegalStateException] {
      succeedingDispatcher(mockIoUtil, testTwoGvcfsLocation)
        .dispatch(goodGvcfDeleteCommand)
    }
  }

  it should "throw an exception if Clio can't delete the cloud file" in {
    recoverToSucceededIf[RuntimeException] {
      succeedingDispatcher(
        new MockIoUtil {
          override def deleteGoogleObject(path: URI): Int = 1
          override def googleObjectExists(path: URI): Boolean = true
        },
        testGvcfLocation
      ).dispatch(goodGvcfDeleteCommand)
    }
  }

  it should "throw an exception if Clio can't delete the gvcf record" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testGvcfCloudSourcePath)
    recoverToSucceededIf[RuntimeException] {
      new CommandDispatch(MockClioWebClient.failingToUpsert, mockIoUtil)
        .dispatch(goodGvcfDeleteCommand)
    }
  }

  it should "throw an exception when attempting to delete a gvcf in Clio if the cloud path does not exist" in {
    recoverToSucceededIf[IllegalStateException] {
      succeedingDispatcher(new MockIoUtil, testGvcfLocation)
        .dispatch(goodGvcfDeleteCommand)
    }
  }

  it should "delete a gvcf in Clio if the cloud gcvf does not exist and the force flag is true" in {
    succeedingDispatcher(new MockIoUtil, testGvcfLocation)
      .dispatch(goodGvcfDeleteCommand.copy(force = true))
      .map(_ shouldBe an[UpsertId])
  }

  it should "delete a gvcf in Clio and the cloud" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testGvcfCloudSourcePath)
    succeedingDispatcher(new MockIoUtil, testGvcfLocation)
      .dispatch(goodGvcfDeleteCommand.copy(force = true))
      .map(_ shouldBe an[UpsertId])
  }
}
