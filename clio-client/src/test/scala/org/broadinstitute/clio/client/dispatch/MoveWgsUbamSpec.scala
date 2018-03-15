package org.broadinstitute.clio.client.dispatch

import java.net.URI

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.commands.MoveWgsUbam
import org.broadinstitute.clio.client.util.MockIoUtil
import org.broadinstitute.clio.client.webclient.MockClioWebClient
import org.broadinstitute.clio.transfer.model.ubam.UbamKey
import org.broadinstitute.clio.util.model.{Location, UpsertId}

class MoveWgsUbamSpec extends BaseClientSpec {
  behavior of "MoveWgsUbam"

  implicit val bearerToken: OAuth2BearerToken = testBearer

  it should "throw an exception if the destination path scheme is invalid" in {
    recoverToSucceededIf[IllegalArgumentException] {
      val command = MoveWgsUbam(
        key = testWgsUbamKey,
        destination = MockIoUtil.InvalidPath
      )
      succeedingDispatcher().dispatch(command)
    }
  }

  it should "throw an exception if the unmapped bam file does not exist" in {
    recoverToSucceededIf[RuntimeException] {
      val command =
        MoveWgsUbam(
          key = testWgsUbamKey,
          destination = testUbamCloudSourcePath
        )
      succeedingDispatcher().dispatch(command)
    }
  }

  it should "throw an exception if Clio returns an error" in {
    recoverToSucceededIf[RuntimeException] {
      val command =
        MoveWgsUbam(
          key = testWgsUbamKey,
          destination = testUbamCloudSourcePath
        )
      failingDispatcher.dispatch(command)
    }
  }

  it should "throw an exception if Clio doesn't return a ubam" in {
    recoverToSucceededIf[IllegalStateException] {
      val command =
        MoveWgsUbam(
          key = testWgsUbamKey,
          destination = testCloudDestinationDirectoryPath
        )
      succeedingDispatcher().dispatch(command)
    }
  }

  it should "throw an exception if Clio can't upsert the new ubam" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testUbamCloudSourcePath)
    recoverToSucceededIf[RuntimeException] {
      new CommandDispatch(MockClioWebClient.failingToUpsert, mockIoUtil)
        .dispatch(goodMoveCommand)
    }
  }

  it should "throw an exception if given a non-GCP unmapped bam" in {
    recoverToSucceededIf[UnsupportedOperationException] {
      val command = MoveWgsUbam(
        key = UbamKey(
          flowcellBarcode = testFlowcell,
          lane = testLane,
          libraryName = testLibName,
          location = Location.OnPrem
        ),
        destination = testCloudDestinationDirectoryPath
      )
      succeedingDispatcher().dispatch(command)
    }
  }

  it should "throw an exception if the destination path is not in GCP" in {
    recoverToSucceededIf[IllegalArgumentException] {
      val command = MoveWgsUbam(
        key = testWgsUbamKey,
        destination = URI.create("/this/is/a/local/path")
      )
      succeedingDispatcher().dispatch(command)
    }
  }
  it should "throw an exception if given a non-directory destination" in {
    recoverToSucceededIf[IllegalArgumentException] {
      val command = MoveWgsUbam(
        key = testWgsUbamKey,
        destination = URI.create("gs://the-bucket/the/file.unmapped.bam")
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
