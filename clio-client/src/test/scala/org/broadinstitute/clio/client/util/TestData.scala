package org.broadinstitute.clio.client.util

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

  val metadataFileLocation =
    "clio-client/src/test/resources/testdata/metadata"

  val testWgsUbamLocation = Some(
    "clio-client/src/test/resources/testdata/testWgsUbam"
  )

  val testTwoWgsUbamsLocation = Some(
    "clio-client/src/test/resources/testdata/testWgsUbams"
  )

  val metadataPlusExtraFieldsFileLocation =
    "clio-client/src/test/resources/testdata/metadataplusextrafields"

  val gvcfMetadataFileLocation =
    "clio-client/src/test/resources/testdata/gvcfMetadata"

  val testGvcfLocation = Some(
    "clio-client/src/test/resources/testdata/testGvcf"
  )

  val testGvcfMetadataOnlyMetricsLocation = Some(
    "clio-client/src/test/resources/testdata/testGvcfMetadataOnlyMetrics"
  )

  val testTwoGvcfsLocation = Some(
    "clio-client/src/test/resources/testdata/testGvcfs"
  )

  val cramMetadataFileLocation =
    "clio-client/src/test/resources/testdata/cramMetadata"

  val testWgsCramLocation = Some(
    "clio-client/src/test/resources/testdata/testWgsCram"
  )

  val gvcfMetadataPlusExtraFieldsFileLocation =
    "clio-client/src/test/resources/testdata/gvcfMetadataplusextrafields"

  val badMetadataFileLocation =
    "clio-client/src/test/resources/testdata/badmetadata"

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

  val testUbamCloudSourcePath: String =
    "gs://testProject/testSample/ubamPath1.unmapped.bam"

  val testUbamCloudDestinationPath: String =
    "gs://testProject/testSample/ubamPath2.unmapped.bam"

  val testGvcfCloudSourcePath: String =
    "gs://testProject/testSample/gvcfPath1.gvcf"

  val testGvcfMetricsCloudSourcePath: String =
    "gs://path/gvcfMetrics1.gvcf"

  val testCloudDestinationDirectoryPath: String =
    "gs://testProject/testSample/moved/"

  val testGvcfCloudDestinationPath: String =
    testCloudDestinationDirectoryPath + "gvcfPath2.gvcf"

  val testCramCloudSourcePath: String =
    "gs://testProject/testSample/cramPath1.cram"

  val testCraiCloudSourcePath: String =
    "gs://testProject/testSample/craiPath1.crai"

  val testWgsMetricsCloudSourcePath: String =
    "gs://testProject/testSample/metrics.wgs_metrics"

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
  val testRequestTimeout: FiniteDuration = 3.seconds
}
