package org.broadinstitute.clio.client.util

import java.net.URI
import java.time.OffsetDateTime

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.clio.client.commands._
import org.broadinstitute.clio.client.webclient.CredentialsGenerator
import org.broadinstitute.clio.transfer.model.arrays.ArraysKey
import org.broadinstitute.clio.transfer.model.gvcf.{
  GvcfExtensions,
  GvcfKey,
  GvcfQueryInput
}
import org.broadinstitute.clio.transfer.model.ubam.{
  UbamKey,
  UbamQueryInput,
  UbamExtensions
}
import org.broadinstitute.clio.transfer.model.wgscram.{
  WgsCramKey,
  WgsCramQueryInput,
  WgsCramExtensions
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

  val testWgsUbamKey = UbamKey(
    flowcellBarcode = testFlowcell,
    lane = testLane,
    libraryName = testLibName,
    location = Location.namesToValuesMap(testLocation)
  )

  val testGvcfKey = GvcfKey(
    location = Location.namesToValuesMap(testLocation),
    project = testProject,
    sampleAlias = testSampleAlias,
    version = testVersion
  )

  val testCramKey = WgsCramKey(
    location = Location.namesToValuesMap(testLocation),
    project = testProject,
    sampleAlias = testSampleAlias,
    version = testVersion
  )

  val goodQueryCommand = QueryWgsUbam(
    queryInput = UbamQueryInput()
  )

  val goodAddCommand =
    AddWgsUbam(metadataLocation = metadataFileLocation, key = testWgsUbamKey)

  val goodMoveCommand = MoveWgsUbam(
    key = testWgsUbamKey,
    destination = testCloudDestinationDirectoryPath
  )

  val goodDeleteCommand =
    DeleteWgsUbam(key = testWgsUbamKey, note = "Good delete for test")

  val testGvcfKey2 = GvcfKey(
    location = Location.GCP,
    project = "project",
    sampleAlias = "sampleAlias",
    version = 17
  )

  val goodGvcfQueryCommand = QueryGvcf(queryInput = GvcfQueryInput())

  val goodGvcfAddCommand = AddGvcf(
    metadataLocation = gvcfMetadataFileLocation,
    key = testGvcfKey
  )

  val goodGvcfMoveCommand = MoveGvcf(
    key = testGvcfKey,
    destination = testCloudDestinationDirectoryPath
  )

  val goodGvcfDeleteCommand =
    DeleteGvcf(key = testGvcfKey2, note = "Good delete for gvcf test")

  val goodCramQueryCommand = QueryWgsCram(
    queryInput = WgsCramQueryInput()
  )

  val goodCramAddCommand = AddWgsCram(
    key = testCramKey,
    metadataLocation = cramMetadataFileLocation
  )

  val goodCramMoveCommand = MoveWgsCram(
    key = testCramKey,
    destination = testCloudDestinationDirectoryPath
  )

  val goodCramDeleteCommand =
    DeleteWgsCram(key = testCramKey, note = "Good delete for test")

  val testChipwellBarcode = 'chipwellBarcode

  val testArraysKey = ArraysKey(
    location = Location.OnPrem,
    chipwellBarcode = testChipwellBarcode,
    version = testVersion
  )

  val testArraysLocation = Some(
    URI.create("clio-client/src/test/resources/testdata/arraysMetadata")
  )

  val arraysMetadataFileLocation: URI =
    URI.create("clio-client/src/test/resources/testdata/arraysMetadata")

  val testArraysCloudSourcePath: URI =
    URI.create("gs://testProject/testSample/Arrays")

  val goodArraysAddCommand = AddArrays(
    metadataLocation = arraysMetadataFileLocation,
    key = testArraysKey
  )

  val goodArraysMoveCommand = MoveArrays(
    key = testArraysKey,
    destination = testCloudDestinationDirectoryPath
  )

  val goodArraysDeleteCommand =
    DeleteArrays(key = testArraysKey, note = "Good delete for test")

  val testServerPort: Int = 8086
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
