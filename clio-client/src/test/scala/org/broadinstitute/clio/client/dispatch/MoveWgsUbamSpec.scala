package org.broadinstitute.clio.client.dispatch

import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.commands.MoveWgsUbam
import org.broadinstitute.clio.client.util.MockIoUtil
import org.broadinstitute.clio.client.webclient.MockClioWebClient
import org.broadinstitute.clio.transfer.model.{
  TransferWgsUbamV1Key,
  TransferWgsUbamV1Metadata
}
import org.broadinstitute.clio.util.model.Location

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken

class MoveWgsUbamSpec extends BaseClientSpec {
  behavior of "MoveWgsUbam"

  implicit val bearerToken: OAuth2BearerToken = testBearer

  it should "throw an exception if the destination path scheme is invalid" in {
    recoverToSucceededIf[Exception] {
      val command = MoveWgsUbam(
        metadata =
          TransferWgsUbamV1Metadata(ubamPath = Some("gs://not_a_valid_path")),
        transferWgsUbamV1Key = testTransferV1Key
      )
      succeedingDispatcher.dispatch(command)
    }
  }

  it should "throw an exception if the unmapped bam file does not exist" in {
    recoverToSucceededIf[Exception] {
      val command =
        MoveWgsUbam(
          metadata =
            TransferWgsUbamV1Metadata(ubamPath = testUbamCloudSourcePath),
          transferWgsUbamV1Key = testTransferV1Key
        )
      succeedingDispatcher.dispatch(command)
    }
  }

  it should "throw an exception if the source and destination paths are the same" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testUbamCloudSourcePath.get)
    recoverToSucceededIf[Exception] {
      val command =
        MoveWgsUbam(
          metadata =
            TransferWgsUbamV1Metadata(ubamPath = testUbamCloudSourcePath),
          transferWgsUbamV1Key = testTransferV1Key
        )
      succeedingReturningDispatcherWgsUbam(mockIoUtil).dispatch(command)
    }
  }

  it should "throw an exception if Clio returns an error" in {
    recoverToSucceededIf[Exception] {
      val command =
        MoveWgsUbam(
          metadata =
            TransferWgsUbamV1Metadata(ubamPath = testUbamCloudSourcePath),
          transferWgsUbamV1Key = testTransferV1Key
        )
      failingDispatcherWgsUbam.dispatch(command)
    }
  }

  it should "throw an exception if Clio doesn't return a Ubam" in {
    recoverToSucceededIf[Exception] {
      val command =
        MoveWgsUbam(
          metadata =
            TransferWgsUbamV1Metadata(ubamPath = testUbamCloudSourcePath),
          transferWgsUbamV1Key = testTransferV1Key
        )
      succeedingDispatcher.dispatch(command)
    }
  }

  it should "throw an exception if Clio can't upsert the new WgsUbam" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testUbamCloudSourcePath.get)
    recoverToSucceededIf[Exception] {
      new CommandDispatch(MockClioWebClient.failingToAddWgsUbam, mockIoUtil)
        .dispatch(goodMoveCommand)
    }
  }

  it should "throw and exception if given a non-GCP unmapped bam" in {
    recoverToSucceededIf[Exception] {
      val command = MoveWgsUbam(
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

  it should "throw and exception if the destination path is not in GCP" in {
    recoverToSucceededIf[Exception] {
      val command = MoveWgsUbam(
        metadata =
          TransferWgsUbamV1Metadata(ubamPath = Some("/this/is/a/local/path")),
        transferWgsUbamV1Key = testTransferV1Key
      )
      succeedingDispatcher.dispatch(command)
    }
  }

  it should "move clio unmapped bams if no errors are encountered" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testUbamCloudSourcePath.get)
    succeedingReturningDispatcherWgsUbam(mockIoUtil)
      .dispatch(goodMoveCommand)
      .map(_.status should be(StatusCodes.OK))
  }
}
