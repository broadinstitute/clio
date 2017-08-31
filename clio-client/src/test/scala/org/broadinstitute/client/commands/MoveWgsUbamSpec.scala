package org.broadinstitute.client.commands

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.clio.util.model.Location
import org.broadinstitute.client.BaseClientSpec
import org.broadinstitute.client.util.MockIoUtil
import org.broadinstitute.client.webclient.MockClioWebClient
import org.broadinstitute.clio.client.commands.{CommandDispatch, MoveWgsUbam}
import org.broadinstitute.clio.transfer.model.{
  TransferWgsUbamV1Key,
  TransferWgsUbamV1Metadata
}

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
      succeedingReturningDispatcher(mockIoUtil).dispatch(command)
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
      failingDispatcher.dispatch(command)
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
    succeedingReturningDispatcher(mockIoUtil)
      .dispatch(goodMoveCommand)
      .map(_.status should be(StatusCodes.OK))
  }
}
