package org.broadinstitute.clio.client.dispatch

import java.net.URI

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.commands.MoveWgsUbam
import org.broadinstitute.clio.client.util.MockIoUtil
import org.broadinstitute.clio.client.webclient.MockClioWebClient
import org.broadinstitute.clio.transfer.model.ubam.TransferUbamV1Key
import org.broadinstitute.clio.util.model.{Location, UpsertId}

class MoveWgsUbamSpec extends BaseClientSpec {
  behavior of "MoveWgsUbam"

  implicit val bearerToken: OAuth2BearerToken = testBearer

  it should "throw an exception if the destination path scheme is invalid" in {
    recoverToSucceededIf[IllegalArgumentException] {
      val command = MoveWgsUbam(
        key = testTransferV1Key,
        destination = MockIoUtil.InvalidPath
      )
      succeedingDispatcher().dispatch(command)
    }
  }

  it should "throw an exception if the unmapped bam file does not exist" in {
    recoverToSucceededIf[Exception] {
      val command =
        MoveWgsUbam(
          key = testTransferV1Key,
          destination = testUbamCloudSourcePath
        )
      succeedingDispatcher().dispatch(command)
    }
  }

  it should "throw an exception if Clio returns an error" in {
    recoverToSucceededIf[Exception] {
      val command =
        MoveWgsUbam(
          key = testTransferV1Key,
          destination = testUbamCloudSourcePath
        )
      failingDispatcher.dispatch(command)
    }
  }

  it should "throw an exception if Clio doesn't return a Ubam" in {
    recoverToSucceededIf[Exception] {
      val command =
        MoveWgsUbam(
          key = testTransferV1Key,
          destination = testUbamCloudSourcePath
        )
      succeedingDispatcher().dispatch(command)
    }
  }

  it should "throw an exception if Clio can't upsert the new WgsUbam" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testUbamCloudSourcePath)
    recoverToSucceededIf[Exception] {
      new CommandDispatch(MockClioWebClient.failingToUpsert, mockIoUtil)
        .dispatch(goodMoveCommand)
    }
  }

  it should "throw an exception if given a non-GCP unmapped bam" in {
    recoverToSucceededIf[IllegalArgumentException] {
      val command = MoveWgsUbam(
        key = TransferUbamV1Key(
          flowcellBarcode = testFlowcell,
          lane = testLane,
          libraryName = testLibName,
          location = Location.OnPrem
        ),
        destination = testUbamCloudDestinationPath
      )
      succeedingDispatcher().dispatch(command)
    }
  }

  it should "throw an exception if the destination path is not in GCP" in {
    recoverToSucceededIf[IllegalArgumentException] {
      val command = MoveWgsUbam(
        key = testTransferV1Key,
        destination = URI.create("/this/is/a/local/path")
      )
      succeedingDispatcher().dispatch(command)
    }
  }
  it should "throw an exception if given a non-directory destination" in {
    recoverToSucceededIf[IllegalArgumentException] {
      val command = MoveWgsUbam(
        key = testTransferV1Key,
        destination = testGvcfCloudDestinationPath
      )
      succeedingDispatcher().dispatch(command)
    }
  }

  it should "move clio unmapped bams if no errors are encountered" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testUbamCloudSourcePath)
    succeedingDispatcher(mockIoUtil, testWgsUbamLocation)
      .dispatch(goodMoveCommand)
      .map {
        case Some(id) => id shouldBe an[UpsertId]
        case other    => fail(s"Expected a Some[UpsertId], got $other")
      }
  }
}
