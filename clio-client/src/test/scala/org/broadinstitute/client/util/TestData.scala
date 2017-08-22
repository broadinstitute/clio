package org.broadinstitute.client.util

import java.time.OffsetDateTime

import org.broadinstitute.clio.client.commands.Commands
import org.broadinstitute.clio.util.model.DocumentStatus

trait TestData {
  val metadataFileLocation = "clio-client/src/test/resources/testdata/metadata"
  val badMetadataFileLocation =
    "clio-client/src/test/resources/testdata/badmetadata"
  val metadataPlusExtraFieldsFileLocation =
    "clio-client/src/test/resources/testdata/metadataplusextrafields"
  val noCommand = Array("")
  val badCommand = Array("badCommand")

  val testBearer = "testBearerToken"
  val testFlowcell = "testFlowcell"
  val testLibName = "testLibName"
  val testLocation = "GCP"
  val testLane = 1
  val testLcSet = "testLcSet"
  val testProject = "testProject"
  val testSampleAlias = "testSampleAlias"
  val testDocumentStatus = DocumentStatus.Normal
  val testRunDateStart: OffsetDateTime = OffsetDateTime.now()
  val testRunDateEnd: OffsetDateTime = testRunDateStart.plusHours(1)

  //missing lane
  val missingRequired = Array(
    Commands.addWgsUbam,
    "-m",
    metadataFileLocation,
    "-f",
    testFlowcell,
    "-n",
    testLibName,
    "--location",
    testLocation
  )

  //missing lane
  val missingOptional = Array(
    Commands.queryWgsUbam,
    "-f",
    testFlowcell,
    "-n",
    testLibName,
    "--location",
    testLocation
  )

  val goodAddCommand = Array(
    Commands.addWgsUbam,
    "-m",
    metadataFileLocation,
    "-f",
    testFlowcell,
    "-l",
    "1",
    "-n",
    testLibName,
    "--location",
    testLocation
  )

}
