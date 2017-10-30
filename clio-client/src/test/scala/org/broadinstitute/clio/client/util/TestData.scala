package org.broadinstitute.clio.client.util

import java.net.URI
import java.time.OffsetDateTime

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.clio.client.commands._
import org.broadinstitute.clio.transfer.model.gvcf.{
  TransferGvcfV1Key,
  TransferGvcfV1QueryInput
}
import org.broadinstitute.clio.transfer.model.wgscram.{
  TransferWgsCramV1Key,
  TransferWgsCramV1QueryInput
}
import org.broadinstitute.clio.transfer.model.wgsubam.{
  TransferWgsUbamV1Key,
  TransferWgsUbamV1QueryInput
}
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

import scala.concurrent.duration._

trait TestData {

  val metadataFileLocation: URI =
    URI.create("clio-client/src/test/resources/testdata/metadata")

  val testWgsUbamLocation = Some(
    URI.create("clio-client/src/test/resources/testdata/testWgsUbam")
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
    URI.create("gs://testProject/testSample/ubamPath1.unmapped.bam")

  val testUbamCloudDestinationPath: URI =
    URI.create("gs://testProject/testSample/ubamPath2.unmapped.bam")

  val testGvcfCloudSourcePath: URI =
    URI.create("gs://testProject/testSample/gvcfPath1.gvcf")

  val testGvcfSummaryMetricsCloudSourcePath: URI =
    URI.create("gs://path/gvcfSummaryMetrics1.gvcf")

  val testGvcfDetailMetricsCloudSourcePath: URI =
    URI.create("gs://path/gvcfDetailMetrics1.gvcf")

  val testCloudDestinationDirectoryPath: URI =
    URI.create("gs://testProject/testSample/moved/")

  val testGvcfCloudDestinationPath: URI =
    testCloudDestinationDirectoryPath.resolve("gvcfPath2.gvcf")

  val testCramCloudSourcePath: URI =
    URI.create("gs://testProject/testSample/cramPath1.cram")

  val testCraiCloudSourcePath: URI =
    URI.create("gs://testProject/testSample/craiPath1.crai")

  val testCramMd5CloudSourcePath: URI =
    URI.create("gs://testProject/testSample/cramPath1.cram.md5")

  val testWgsMetricsCloudSourcePath: URI =
    URI.create("gs://testProject/testSample/metrics.wgs_metrics")

  val testCommon = CommonOptions(bearerToken = Some(testBearer))
  val testCommonNoToken = CommonOptions()
  val testTransferV1Key = TransferWgsUbamV1Key(
    flowcellBarcode = testFlowcell,
    lane = testLane,
    libraryName = testLibName,
    location = Location.pathMatcher(testLocation)
  )
  val testGvcfTransferV1Key = TransferGvcfV1Key(
    location = Location.pathMatcher(testLocation),
    project = testProject,
    sampleAlias = testSampleAlias,
    version = testVersion
  )
  val testCramTransferV1Key = TransferWgsCramV1Key(
    location = Location.pathMatcher(testLocation),
    project = testProject,
    sampleAlias = testSampleAlias,
    version = testVersion
  )

  val goodQueryCommand = QueryWgsUbam(
    queryInput = TransferWgsUbamV1QueryInput()
  )

  val goodAddCommand =
    AddWgsUbam(metadataLocation = metadataFileLocation, key = testTransferV1Key)

  val goodMoveCommand = MoveWgsUbam(
    key = testTransferV1Key,
    destination = testUbamCloudDestinationPath
  )

  val goodDeleteCommand =
    DeleteWgsUbam(key = testTransferV1Key, note = "Good delete for test")

  val goodGvcfQueryCommand = QueryGvcf(queryInput = TransferGvcfV1QueryInput())

  val goodGvcfAddCommand = AddGvcf(
    metadataLocation = gvcfMetadataFileLocation,
    key = testGvcfTransferV1Key
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
}
