package org.broadinstitute.clio.client.dispatch

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.commands.MoveGvcf
import org.broadinstitute.clio.client.util.MockIoUtil
import org.broadinstitute.clio.client.webclient.MockClioWebClient
import org.broadinstitute.clio.transfer.model.gvcf.TransferGvcfV1Key
import org.broadinstitute.clio.util.model.Location

class MoveGvcfSpec extends BaseClientSpec {
  behavior of "MoveGvcf"

  implicit val bearerToken: OAuth2BearerToken = testBearer

  it should "throw an exception if the destination path scheme is invalid" in {
    recoverToSucceededIf[Exception] {
      val command = MoveGvcf(
        key = testGvcfTransferV1Key,
        destination = "gs://not_a_valid_path"
      )
      succeedingDispatcher.dispatch(command)
    }
  }

  it should "throw an exception if the gvcf file does not exist" in {
    recoverToSucceededIf[Exception] {
      val command =
        MoveGvcf(
          key = testGvcfTransferV1Key,
          destination = testGvcfCloudSourcePath
        )
      succeedingDispatcher.dispatch(command)
    }
  }

  it should "throw an exception if the source and destination paths are the same" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testGvcfCloudSourcePath)
    recoverToSucceededIf[Exception] {
      val command = MoveGvcf(
        key = testGvcfTransferV1Key,
        destination = testGvcfCloudSourcePath
      )
      succeedingReturningDispatcherGvcf(mockIoUtil).dispatch(command)
    }
  }

  it should "throw an exception if Clio doesn't return a Gvcf" in {
    recoverToSucceededIf[Exception] {
      val command =
        MoveGvcf(
          key = testGvcfTransferV1Key,
          destination = testGvcfCloudSourcePath
        )
      succeedingDispatcher.dispatch(command)
    }
  }

  it should "throw an exception if Clio can't upsert the new Gvcf" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testGvcfCloudSourcePath)
    recoverToSucceededIf[Exception] {
      new CommandDispatch(MockClioWebClient.failingToUpsert, mockIoUtil)
        .dispatch(goodGvcfMoveCommand)
    }
  }

  it should "throw an exception if given a non-GCP gvcf" in {
    recoverToSucceededIf[Exception] {
      val command = MoveGvcf(
        key = TransferGvcfV1Key(
          location = Location.OnPrem,
          project = testProject,
          sampleAlias = testSampleAlias,
          version = testVersion
        ),
        destination = testGvcfCloudDestinationDirectoryPath
      )
      succeedingDispatcher.dispatch(command)
    }
  }

  it should "throw an exception if the destination path is not in GCP" in {
    recoverToSucceededIf[Exception] {
      val command = MoveGvcf(
        key = testGvcfTransferV1Key,
        destination = "/this/is/a/local/path"
      )
      succeedingDispatcher.dispatch(command)
    }
  }

  it should "throw an exception if given a non-directory destination for multiple source files" in {
    recoverToSucceededIf[Exception] {
      val command = MoveGvcf(
        key = testGvcfTransferV1Key,
        destination = testGvcfCloudDestinationPath
      )
      succeedingDispatcher.dispatch(command)
    }
  }

  it should "move clio gvcfs if no errors are encountered" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testGvcfCloudSourcePath)
    succeedingReturningDispatcherGvcf(mockIoUtil)
      .dispatch(goodGvcfMoveCommand)
      .map(_.status should be(StatusCodes.OK))
  }

  it should "move to a non-directory destination if only one file is associated with a gvcf key" in {
    val mockIoUtil = new MockIoUtil
    val dispatcher = new CommandDispatch(
      MockClioWebClient.returningGvcfOnlyMetadata,
      mockIoUtil
    )
    mockIoUtil.putFileInCloud(testGvcfCloudSourcePath)
    dispatcher
      .dispatch(
        MoveGvcf(
          key = testGvcfTransferV1Key,
          destination = testGvcfCloudDestinationPath
        )
      )
      .map(_.status should be(StatusCodes.OK))
  }
}
