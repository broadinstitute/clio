package org.broadinstitute.client.util

import java.time.OffsetDateTime

import org.broadinstitute.clio.client.commands.{
  AddWgsUbam,
  CommonOptions,
  QueryWgsUbam
}
import org.broadinstitute.clio.transfer.model.{
  TransferWgsUbamV1Key,
  TransferWgsUbamV1QueryInput
}
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

trait TestData {

  val metadataFileLocation =
    "clio-client/src/test/resources/testdata/metadata"
  val badMetadataFileLocation =
    "clio-client/src/test/resources/testdata/badmetadata"
  val metadataPlusExtraFieldsFileLocation =
    "clio-client/src/test/resources/testdata/metadataplusextrafields"

  val testBearer = "testBearerToken"
  val testFlowcell = "testFlowcell"
  val testLibName = "testLibName"
  val testLocation = "GCP"
  val testLane = 1
  val testLcSet = "testLcSet"
  val testProject = "testProject"
  val testSampleAlias = "testSampleAlias"
  val testDocumentStatus = DocumentStatus.Normal
  val testRunDateStart: OffsetDateTime =OffsetDateTime.now()
  val testRunDateEnd: OffsetDateTime = OffsetDateTime.now().plusHours(1)


  val testCommon = CommonOptions(bearerToken = testBearer)
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

}
