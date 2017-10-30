package org.broadinstitute.clio.client.dispatch

import java.net.URI

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.commands.MoveWgsCram
import org.broadinstitute.clio.client.util.MockIoUtil
import org.broadinstitute.clio.client.webclient.MockClioWebClient
import org.broadinstitute.clio.util.model.Location

class MoveWgsCramSpec extends BaseClientSpec {
  behavior of "MoveWgsCram"

  implicit val bearerToken: OAuth2BearerToken = testBearer

  it should "throw an exception if the destination path scheme is invalid" in {
    recoverToSucceededIf[Exception] {
      val command = MoveWgsCram(
        key = testCramTransferV1Key,
        destination = MockIoUtil.InvalidPath
      )
      succeedingDispatcher.dispatch(command)
    }
  }

  it should "throw an exception if the cram file does not exist" in {
    recoverToSucceededIf[Exception] {
      val command =
        MoveWgsCram(
          key = testCramTransferV1Key,
          destination = testUbamCloudSourcePath
        )
      succeedingDispatcher.dispatch(command)
    }
  }

  it should "throw an exception if Clio returns an error" in {
    recoverToSucceededIf[Exception] {
      val command =
        MoveWgsCram(
          key = testCramTransferV1Key,
          destination = testUbamCloudSourcePath
        )
      failingDispatcher.dispatch(command)
    }
  }

  it should "throw an exception if Clio doesn't return a Ubam" in {
    recoverToSucceededIf[Exception] {
      val command =
        MoveWgsCram(
          key = testCramTransferV1Key,
          destination = testUbamCloudSourcePath
        )
      succeedingDispatcher.dispatch(command)
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

  it should "throw an exception if given a non-GCP cram" in {
    recoverToSucceededIf[Exception] {
      val command = MoveWgsCram(
        key = testCramTransferV1Key.copy(location = Location.OnPrem),
        destination = testUbamCloudDestinationPath
      )
      succeedingDispatcher.dispatch(command)
    }
  }

  it should "throw an exception if the destination path is not in GCP" in {
    recoverToSucceededIf[Exception] {
      val command = MoveWgsCram(
        key = testCramTransferV1Key,
        destination = URI.create("/this/is/a/local/path")
      )
      succeedingDispatcher.dispatch(command)
    }
  }

  it should "move clio crams if no errors are encountered" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testCramCloudSourcePath)
    mockIoUtil.putFileInCloud(testCraiCloudSourcePath)
    mockIoUtil.putFileInCloud(testCramMd5CloudSourcePath)
    succeedingReturningDispatcherWgsCram(mockIoUtil)
      .dispatch(goodCramMoveCommand)
      .map(_.status should be(StatusCodes.OK))
  }

  it should "only move the cram, crai, and md5" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testCramCloudSourcePath)
    mockIoUtil.putFileInCloud(testCraiCloudSourcePath)
    mockIoUtil.putFileInCloud(testCramMd5CloudSourcePath)
    mockIoUtil.putFileInCloud(testWgsMetricsCloudSourcePath)

    succeedingReturningDispatcherWgsCram(mockIoUtil)
      .dispatch(goodCramMoveCommand)
      .map(_.status should be(StatusCodes.OK)).andThen {
      case _  =>
        mockIoUtil.googleObjectExists(URI.create(s"${testCloudDestinationDirectoryPath}cramPath1.cram")) should be(true)
        mockIoUtil.googleObjectExists(URI.create(s"${testCloudDestinationDirectoryPath}craiPath1.crai")) should be(true)
        mockIoUtil.googleObjectExists(URI.create(s"${testCloudDestinationDirectoryPath}cramPath1.cram.md5")) should be(true)
        mockIoUtil.googleObjectExists(URI.create(s"${testCloudDestinationDirectoryPath}metrics.wgs_metrics")) should be(false)

    }


  }
}
