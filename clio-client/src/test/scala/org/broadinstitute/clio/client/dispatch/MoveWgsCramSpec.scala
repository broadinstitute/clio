package org.broadinstitute.clio.client.dispatch

import java.net.URI

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.commands.MoveWgsCram
import org.broadinstitute.clio.client.util.MockIoUtil
import org.broadinstitute.clio.client.webclient.MockClioWebClient
import org.broadinstitute.clio.transfer.model.wgscram.WgsCramExtensions
import org.broadinstitute.clio.util.model.{Location, UpsertId}

class MoveWgsCramSpec extends BaseClientSpec {
  behavior of "MoveWgsCram"

  implicit val bearerToken: OAuth2BearerToken = testBearer

  it should "throw an exception if the destination path scheme is invalid" in {
    recoverToSucceededIf[IllegalArgumentException] {
      val command = MoveWgsCram(
        key = testCramTransferV1Key,
        destination = MockIoUtil.InvalidPath
      )
      succeedingDispatcher().dispatch(command)
    }
  }

  it should "throw an exception if the cram file does not exist" in {
    recoverToSucceededIf[Exception] {
      val command =
        MoveWgsCram(
          key = testCramTransferV1Key,
          destination = testUbamCloudSourcePath
        )
      succeedingDispatcher().dispatch(command)
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

  it should "throw an exception if Clio doesn't return a cram" in {
    recoverToSucceededIf[Exception] {
      val command =
        MoveWgsCram(
          key = testCramTransferV1Key,
          destination = testUbamCloudSourcePath
        )
      succeedingDispatcher().dispatch(command)
    }
  }

  it should "throw an exception if Clio can't upsert the new cram" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testUbamCloudSourcePath)
    recoverToSucceededIf[Exception] {
      new CommandDispatch(MockClioWebClient.failingToUpsert, mockIoUtil)
        .dispatch(goodMoveCommand)
    }
  }

  it should "throw an exception if given a non-GCP cram" in {
    recoverToSucceededIf[IllegalArgumentException] {
      val command = MoveWgsCram(
        key = testCramTransferV1Key.copy(location = Location.OnPrem),
        destination = testUbamCloudDestinationPath
      )
      succeedingDispatcher().dispatch(command)
    }
  }

  it should "throw an exception if the destination path is not in GCP" in {
    recoverToSucceededIf[IllegalArgumentException] {
      val command = MoveWgsCram(
        key = testCramTransferV1Key,
        destination = URI.create("/this/is/a/local/path")
      )
      succeedingDispatcher().dispatch(command)
    }
  }

  it should "move clio crams if no errors are encountered" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testCramCloudSourcePath)
    mockIoUtil.putFileInCloud(testCraiCloudSourcePath)
    succeedingDispatcher(mockIoUtil, testWgsCramLocation)
      .dispatch(goodCramMoveCommand)
      .map {
        case Some(id) => id shouldBe an[UpsertId]
        case other    => fail(s"Expected a Some[UpsertId], got $other")
      }
  }

  it should "only move the cram and crai" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testCramCloudSourcePath)
    mockIoUtil.putFileInCloud(testCraiCloudSourcePath)
    mockIoUtil.putFileInCloud(testWgsMetricsCloudSourcePath)
    succeedingDispatcher(mockIoUtil, testWgsCramLocation)
      .dispatch(goodCramMoveCommand)
      .map { response =>
        response match {
          case Some(id) => id shouldBe an[UpsertId]
          case other    => fail(s"Expected a Sine[UpsertId], got $other")
        }

        mockIoUtil.googleObjectExists(
          URI.create(
            s"${testCloudDestinationDirectoryPath}cramPath1${WgsCramExtensions.CramExtension}"
          )
        ) should be(true)
        mockIoUtil.googleObjectExists(
          URI.create(
            s"${testCloudDestinationDirectoryPath}cramPath1${WgsCramExtensions.CraiExtension}"
          )
        ) should be(true)
        mockIoUtil.googleObjectExists(
          URI.create(s"${testCloudDestinationDirectoryPath}metrics.wgs_metrics")
        ) should be(false)
      }
  }

  it should s"change the crai extension to be ${WgsCramExtensions.CraiExtension}" in {
    val oldStyleCrai =
      URI.create(
        testCramCloudSourcePath.toString.replaceAll(
          s"\\${WgsCramExtensions.CramExtension}",
          WgsCramExtensions.CraiExtensionAddition
        )
      )

    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testCramCloudSourcePath)
    mockIoUtil.putFileInCloud(oldStyleCrai)
    mockIoUtil.putFileInCloud(testWgsMetricsCloudSourcePath)

    succeedingDispatcher(mockIoUtil, testWgsCramWithOldExtensionLocation)
      .dispatch(goodCramMoveCommand)
      .map { response =>
        response match {
          case Some(id) => id shouldBe an[UpsertId]
          case other    => fail(s"Expected a Some[UpsertId], got $other")
        }

        mockIoUtil.googleObjectExists(
          URI.create(
            s"${testCloudDestinationDirectoryPath}cramPath1${WgsCramExtensions.CramExtension}"
          )
        ) should be(true)
        mockIoUtil.googleObjectExists(
          URI.create(
            s"${testCloudDestinationDirectoryPath}cramPath1${WgsCramExtensions.CraiExtension}"
          )
        ) should be(true)
        mockIoUtil.googleObjectExists(
          URI.create(s"${testCloudDestinationDirectoryPath}metrics.wgs_metrics")
        ) should be(false)
      }
  }
}
