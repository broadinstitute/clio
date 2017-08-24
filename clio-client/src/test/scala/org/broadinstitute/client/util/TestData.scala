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

  val metadataFileLocation = Some(
    "clio-client/src/test/resources/testdata/metadata"
  )
  val badMetadataFileLocation =
    Some("clio-client/src/test/resources/testdata/badmetadata")
  val metadataPlusExtraFieldsFileLocation =
    Some("clio-client/src/test/resources/testdata/metadataplusextrafields")
  val noCommand = Array("")
  val badCommand = Array("badCommand")

  val testBearer = Some("testBearerToken")
  val testFlowcell = Some("testFlowcell")
  val testLibName = Some("testLibName")
  val testLocation = Some("GCP")
  val testLane = Some(1)
  val testLcSet = Some("testLcSet")
  val testProject = Some("testProject")
  val testSampleAlias = Some("testSampleAlias")
  val testDocumentStatus = Some(DocumentStatus.Normal)
  val testRunDateStart: Option[OffsetDateTime] = Some(OffsetDateTime.now())
  val testRunDateEnd: Option[OffsetDateTime] = Some(
    OffsetDateTime.now().plusHours(1)
  )
  val testCommon = CommonOptions(bearerToken = testBearer.get)
  val testTransferV1Key = TransferWgsUbamV1Key(
    flowcellBarcode = testFlowcell.get,
    lane = testLane.get,
    libraryName = testLibName.get,
    location = Location.pathMatcher(testLocation.get)
  )

  //missing lane
  val missingRequired = Array(
    // Commands.AddWgsUbam.toString,
    "--meta-data-",
    metadataFileLocation.get,
    "-f",
    testFlowcell.get,
    "-n",
    testLibName.get,
    "--location",
    testLocation.get
  )

  //missing lane
  val missingOptional = Array(
    // Commands.QueryWgsUbam.toString,
    "-f",
    testFlowcell.get,
    "-n",
    testLibName.get,
    "--location",
    testLocation.get
  )

  val goodQueryCommand = QueryWgsUbam(
    transferWgsUbamV1QueryInput = TransferWgsUbamV1QueryInput()
  )

  val goodAddCommand = AddWgsUbam(
    metadataLocation = metadataFileLocation.get,
    transferWgsUbamV1Key = testTransferV1Key
  )

}
