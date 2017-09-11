package org.broadinstitute.clio.client.util

import org.broadinstitute.clio.client.commands._
import org.broadinstitute.clio.transfer.model._
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

import akka.http.scaladsl.model.headers.OAuth2BearerToken

import scala.concurrent.duration._

import java.time.OffsetDateTime

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

  val testTwoGvcfsLocation = Some(
    "clio-client/src/test/resources/testdata/testGvcfs"
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

  val testUbamCloudSourcePath: Option[String] =
    Some("gs://testProject/testSample/ubamPath1.unmapped.bam")
  val testUbamCloudDestinationPath: Option[String] =
    Some("gs://testProject/testSample/ubamPath2.unmapped.bam")
  val testGvcfCloudSourcePath: Option[String] =
    Some("gs://testProject/testSample/gvcfPath1.gvcf")
  val testGvcfCloudDestinationPath: Option[String] =
    Some("gs://testProject/testSample/gvcfPath2.gvcf")

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

  val goodQueryCommand = QueryWgsUbam(
    transferWgsUbamV1QueryInput = TransferWgsUbamV1QueryInput()
  )

  val goodAddCommand = AddWgsUbam(
    metadataLocation = metadataFileLocation,
    transferWgsUbamV1Key = testTransferV1Key
  )

  val goodMoveCommand = MoveWgsUbam(
    metadata =
      TransferWgsUbamV1Metadata(ubamPath = testUbamCloudDestinationPath),
    transferWgsUbamV1Key = testTransferV1Key
  )

  val goodDeleteCommand = DeleteWgsUbam(
    metadata =
      TransferWgsUbamV1Metadata(ubamPath = testUbamCloudDestinationPath),
    transferWgsUbamV1Key = testTransferV1Key
  )

  val goodGvcfQueryCommand = QueryGvcf(
    transferGvcfV1QueryInput = TransferGvcfV1QueryInput()
  )

  val goodGvcfAddCommand = AddGvcf(
    metadataLocation = gvcfMetadataFileLocation,
    transferGvcfV1Key = testGvcfTransferV1Key
  )

  val goodGvcfMoveCommand = MoveGvcf(
    metadata = TransferGvcfV1Metadata(gvcfPath = testGvcfCloudDestinationPath),
    transferGvcfV1Key = testGvcfTransferV1Key
  )

  val goodGvcfDeleteCommand = DeleteGvcf(
    metadata = TransferGvcfV1Metadata(gvcfPath = testGvcfCloudDestinationPath),
    transferGvcfV1Key = testGvcfTransferV1Key
  )

  val testServerPort: Int = 8080
  val testRequestTimeout: FiniteDuration = 3.seconds
}
