package org.broadinstitute.clio.client.dispatch

import java.net.URI

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.commands.MoveGvcf
import org.broadinstitute.clio.client.util.MockIoUtil
import org.broadinstitute.clio.client.webclient.MockClioWebClient
import org.broadinstitute.clio.transfer.model.gvcf.TransferGvcfV1Key
import org.broadinstitute.clio.util.model.{Location, UpsertId}

class MoveGvcfSpec extends BaseClientSpec {
  behavior of "MoveGvcf"

  implicit val bearerToken: OAuth2BearerToken = testBearer

  it should "throw an exception if the destination path scheme is invalid" in {
    recoverToSucceededIf[IllegalArgumentException] {
      val command = MoveGvcf(
        key = testGvcfTransferV1Key,
        destination = MockIoUtil.InvalidPath
      )
      succeedingDispatcher().dispatch(command)
    }
  }

  it should "throw an exception if the gvcf file does not exist" in {
    recoverToSucceededIf[RuntimeException] {
      val command =
        MoveGvcf(
          key = testGvcfTransferV1Key,
          destination = testGvcfCloudSourcePath
        )
      succeedingDispatcher().dispatch(command)
    }
  }

  it should "throw an exception if Clio doesn't return a gvcf" in {
    recoverToSucceededIf[IllegalStateException] {
      val command =
        MoveGvcf(
          key = testGvcfTransferV1Key,
          destination = testCloudDestinationDirectoryPath
        )
      succeedingDispatcher().dispatch(command)
    }
  }

  it should "throw an exception if Clio can't upsert a new gvcf record" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testGvcfCloudSourcePath)
    recoverToSucceededIf[RuntimeException] {
      new CommandDispatch(MockClioWebClient.failingToUpsert, mockIoUtil)
        .dispatch(goodGvcfMoveCommand)
    }
  }

  it should "throw an exception if given a non-GCP gvcf" in {
    recoverToSucceededIf[UnsupportedOperationException] {
      val command = MoveGvcf(
        key = TransferGvcfV1Key(
          location = Location.OnPrem,
          project = testProject,
          sampleAlias = testSampleAlias,
          version = testVersion
        ),
        destination = testCloudDestinationDirectoryPath
      )
      succeedingDispatcher().dispatch(command)
    }
  }

  it should "throw an exception if the destination path is not in GCP" in {
    recoverToSucceededIf[IllegalArgumentException] {
      val command = MoveGvcf(
        key = testGvcfTransferV1Key,
        destination = URI.create("/this/is/a/local/path")
      )
      succeedingDispatcher().dispatch(command)
    }
  }

  it should "throw an exception if given a non-directory destination" in {
    recoverToSucceededIf[IllegalArgumentException] {
      val command = MoveGvcf(
        key = testGvcfTransferV1Key,
        destination = URI.create("gs://the-bucket/the/file.g.vcf.gz")
      )
      succeedingDispatcher().dispatch(command)
    }
  }

  it should "move clio gvcfs if no errors are encountered" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testGvcfCloudSourcePath)
    mockIoUtil.putFileInCloud(testGvcfSummaryMetricsCloudSourcePath)
    mockIoUtil.putFileInCloud(testGvcfDetailMetricsCloudSourcePath)
    succeedingDispatcher(mockIoUtil, testGvcfLocation)
      .dispatch(goodGvcfMoveCommand)
      .map {
        case Some(id) => id shouldBe an[UpsertId]
        case other    => fail(s"Expected a Some[UpsertId], got $other")
      }
  }
}
