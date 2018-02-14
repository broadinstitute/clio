package org.broadinstitute.clio.client.util

import java.net.URI
import java.time.OffsetDateTime

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.clio.client.commands._
import org.broadinstitute.clio.client.webclient.CredentialsGenerator
import org.broadinstitute.clio.transfer.model.gvcf.{
  GvcfExtensions,
  TransferGvcfV1Key,
  TransferGvcfV1QueryInput
}
import org.broadinstitute.clio.transfer.model.wgscram.{
  TransferWgsCramV1Key,
  TransferWgsCramV1QueryInput,
  WgsCramExtensions
}
import org.broadinstitute.clio.transfer.model.ubam.{
  TransferUbamV1Key,
  TransferUbamV1QueryInput,
  UbamExtensions
}
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

import scala.concurrent.duration._

trait TestData {

  val metadataFileLocation: URI =
    URI.create("clio-client/src/test/resources/testdata/metadata")

  val testWgsUbamLocation = Some(
    URI.create("clio-client/src/test/resources/testdata/testWgsUbam")
  )

  val testWgsChangedUbamLocation = Some(
    URI.create("clio-client/src/test/resources/testdata/testChangedWgsUbam")
  )

  val testTwoWgsUbamsLocation = Some(
    URI.create("clio-client/src/test/resources/testdata/testWgsUbams")
  )

  val metadataPlusExtraFieldsFileLocation: URI =
    URI.create(
      "clio-client/src/test/resources/testdata/metadataplusextrafields"
    )

  val gvcfMetadataFileLocation: URI =
    URI.create("clio-client/src/test/resources/testdata/gvcfMetadata")

  val testGvcfLocation = Some(
    URI.create("clio-client/src/test/resources/testdata/testGvcf")
  )

  val testGvcfMetadataOnlyMetricsLocation = Some(
    URI.create(
      "clio-client/src/test/resources/testdata/testGvcfMetadataOnlyMetrics"
    )
  )

  val testGvcfMetadataOnlyOneMetricLocation = Some(
    URI.create(
      "clio-client/src/test/resources/testdata/testGvcfMetadataOnlyOneMetric"
    )
  )

  val testTwoGvcfsLocation = Some(
    URI.create("clio-client/src/test/resources/testdata/testGvcfs")
  )

  val cramMetadataFileLocation: URI =
    URI.create("clio-client/src/test/resources/testdata/cramMetadata")

  val testWgsCramLocation = Some(
    URI.create("clio-client/src/test/resources/testdata/testWgsCram")
  )

  val testWgsCramWithOldExtensionLocation = Some(
    URI.create(
      "clio-client/src/test/resources/testdata/testWgsCramOldIndexExtension"
    )
  )

  val gvcfMetadataPlusExtraFieldsFileLocation: URI =
    URI.create(
      "clio-client/src/test/resources/testdata/gvcfMetadataplusextrafields"
    )

  val badMetadataFileLocation: URI =
    URI.create("clio-client/src/test/resources/testdata/badmetadata")

  val testBearer = OAuth2BearerToken("testBearerToken")
  val testFlowcell = "testFlowcell"
  val testLibName = "testLibName"
  val testLocation = "GCP"
  val testLane = 1
  val testLcSet = "testLcSet"
  val testProject = "testProject"
  val testSampleAlias = "testSampleAlias"
  val testVersion = 1
  val testDocumentStatus: DocumentStatus.Normal.type = DocumentStatus.Normal
  val testRunDateStart: OffsetDateTime = OffsetDateTime.now()
  val testRunDateEnd: OffsetDateTime = OffsetDateTime.now().plusHours(1)

  val testUbamCloudSourcePath: URI =
    URI.create(
      s"gs://testProject/testSample/ubamPath1${UbamExtensions.UbamExtension}"
    )

  val testUbamCloudDestinationPath: URI =
    URI.create(
      s"gs://testProject/testSample/ubamPath2${UbamExtensions.UbamExtension}"
    )

  val testGvcfCloudSourcePath: URI =
    URI.create(
      s"gs://testProject/testSample/gvcfPath1${GvcfExtensions.GvcfExtension}"
    )

  val testGvcfSummaryMetricsCloudSourcePath: URI =
    URI.create(
      s"gs://path/gvcfSummaryMetrics1${GvcfExtensions.SummaryMetricsExtension}"
    )

  val testGvcfDetailMetricsCloudSourcePath: URI =
    URI.create(
      s"gs://path/gvcfDetailMetrics1${GvcfExtensions.DetailMetricsExtension}"
    )

  val testCloudDestinationDirectoryPath: URI =
    URI.create("gs://testProject/testSample/moved/")

  val testGvcfCloudDestinationPath: URI =
    testCloudDestinationDirectoryPath.resolve(
      s"gvcfPath2${GvcfExtensions.GvcfExtension}"
    )

  val testCramCloudSourcePath: URI =
    URI.create(
      s"gs://testProject/testSample/cramPath1${WgsCramExtensions.CramExtension}"
    )

  val testCraiCloudSourcePath: URI =
    URI.create(
      s"gs://testProject/testSample/cramPath1${WgsCramExtensions.CraiExtension}"
    )

  val testWgsMetricsCloudSourcePath: URI =
    URI.create("gs://testProject/testSample/cramPath1.wgs_metrics")

  val testTransferV1Key = TransferUbamV1Key(
    flowcellBarcode = testFlowcell,
    lane = testLane,
    libraryName = testLibName,
    location = Location.namesToValuesMap(testLocation)
  )

  val testGvcfTransferV1Key = TransferGvcfV1Key(
    location = Location.namesToValuesMap(testLocation),
    project = testProject,
    sampleAlias = testSampleAlias,
    version = testVersion
  )

  val testCramTransferV1Key = TransferWgsCramV1Key(
    location = Location.namesToValuesMap(testLocation),
    project = testProject,
    sampleAlias = testSampleAlias,
    version = testVersion
  )

  val goodQueryCommand = QueryWgsUbam(
    queryInput = TransferUbamV1QueryInput()
  )

  val goodAddCommand =
    AddWgsUbam(metadataLocation = metadataFileLocation, key = testTransferV1Key)

  val goodAddCommandForceUpdate =
    AddWgsUbam(
      metadataLocation = metadataFileLocation,
      key = testTransferV1Key,
      forceUpdate = true
    )

  val goodMoveCommand = MoveWgsUbam(
    key = testTransferV1Key,
    destination = testCloudDestinationDirectoryPath
  )

  val goodDeleteCommand =
    DeleteWgsUbam(key = testTransferV1Key, note = "Good delete for test")

  val goodGvcfQueryCommand = QueryGvcf(queryInput = TransferGvcfV1QueryInput())

  val goodGvcfAddCommand = AddGvcf(
    metadataLocation = gvcfMetadataFileLocation,
    key = testGvcfTransferV1Key
  )

  val goodGvcfAddCommandForceUpdate = AddGvcf(
    metadataLocation = gvcfMetadataFileLocation,
    key = testGvcfTransferV1Key,
    forceUpdate = true
  )

  val goodGvcfMoveCommand = MoveGvcf(
    key = testGvcfTransferV1Key,
    destination = testCloudDestinationDirectoryPath
  )

  val goodGvcfDeleteCommand =
    DeleteGvcf(key = testGvcfTransferV1Key, note = "Good delete for test")

  val goodCramQueryCommand = QueryWgsCram(
    queryInput = TransferWgsCramV1QueryInput()
  )

  val goodCramAddCommand = AddWgsCram(
    key = testCramTransferV1Key,
    metadataLocation = cramMetadataFileLocation
  )

  val goodCramMoveCommand = MoveWgsCram(
    key = testCramTransferV1Key,
    destination = testCloudDestinationDirectoryPath
  )

  val goodCramDeleteCommand =
    DeleteWgsCram(key = testCramTransferV1Key, note = "Good delete for test")

  val testServerPort: Int = 8080
  val testMaxQueued: Int = 4
  val testMaxConcurrent: Int = 1
  val testRequestTimeout: FiniteDuration = 3.seconds
  val testMaxRetries = 2

  val fakeTokenGenerator: CredentialsGenerator =
    () => OAuth2BearerToken("fake-token")
}

/**
  * Trait as object, for situations where it's more convenient to
  * import a piece of test data instead of mixing in all fields
  * through inheritence.
  */
object TestData extends TestData
