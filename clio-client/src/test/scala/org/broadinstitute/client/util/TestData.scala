package org.broadinstitute.client.util

import java.time.OffsetDateTime

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.clio.client.commands.Commands.CommonOptions
import org.broadinstitute.clio.client.commands.{
  AddWgsUbam,
  DeleteWgsUbam,
  MoveWgsUbam,
  QueryWgsUbam
}
import org.broadinstitute.clio.transfer.model.{
  TransferWgsUbamV1Key,
  TransferWgsUbamV1Metadata,
  TransferWgsUbamV1QueryInput
}
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

trait TestData {

  val metadataFileLocation =
    "clio-client/src/test/resources/testdata/metadata"

  val testWgsUbamLocation = Some(
    "clio-client/src/test/resources/testdata/testWgsUbam"
  )

  val testTwoWgsUbamsLocation = Some(
    "clio-client/src/test/resources/testdata/testWgsUbams"
  )

  val badMetadataFileLocation =
    "clio-client/src/test/resources/testdata/badmetadata"
  val metadataPlusExtraFieldsFileLocation =
    "clio-client/src/test/resources/testdata/metadataplusextrafields"

  val testBearer = OAuth2BearerToken("testBearerToken")
  val testFlowcell = "testFlowcell"
  val testLibName = "testLibName"
  val testLocation = "GCP"
  val testLane = 1
  val testLcSet = "testLcSet"
  val testProject = "testProject"
  val testSampleAlias = "testSampleAlias"
  val testDocumentStatus: DocumentStatus.Normal.type = DocumentStatus.Normal
  val testRunDateStart: OffsetDateTime = OffsetDateTime.now()
  val testRunDateEnd: OffsetDateTime = OffsetDateTime.now().plusHours(1)

  val testUbamCloudSourcePath: Option[String] =
    Some("gs://testProject/testSample/ubamPath1.unmapped.bam")
  val testUbamCloudDestinationPath: Option[String] =
    Some("gs://testProject/testSample/ubamPath2.unmapped.bam")

  val testCommon = CommonOptions(bearerToken = Some(testBearer))
  val testCommonNoToken = CommonOptions()
  val testTransferV1Key = TransferWgsUbamV1Key(
    flowcellBarcode = testFlowcell,
    lane = testLane,
    libraryName = testLibName,
    location = Location.pathMatcher(testLocation)
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
}
