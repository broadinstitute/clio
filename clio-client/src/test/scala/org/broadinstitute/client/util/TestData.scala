package org.broadinstitute.client.util

import java.time.OffsetDateTime

import org.broadinstitute.clio.client.commands.Commands

trait TestData {
  import scala.concurrent.ExecutionContext.Implicits.global

  val metadataFileLocation = "clio-client/src/test/resources/testdata/metadata"
  val badMetadataFileLocation =
    "clio-client/src/test/resources/testdata/badmetadata"
  val metadataPlusExtraFieldsFileLocation =
    "clio-client/src/test/resources/testdata/metadataplusextrafields"
  val noCommand = Array("")
  val badCommand = Array("badCommand")

  val testBearer = "testBearerToken"
  val testFlowcell = Some("testFlowcell")
  val testLibName = Some("testLibName")
  val testLocation = Some("GCP")
  val testLane = Some(1)
  val testLcSet = Some("testLcSet")
  val testProject = Some("testProject")
  val testSampleAlias = Some("testSampleAlias")
  val testRunDateStart: Option[OffsetDateTime] = Some(OffsetDateTime.now())
  val testRunDateEnd: Option[OffsetDateTime] = Some(
    OffsetDateTime.now().plusHours(1)
  )

  //missing lane
  val missingRequired = Array(
    Commands.AddReadGroupBam.apply().commandName,
    "-m",
    metadataFileLocation,
    "-f",
    testFlowcell.get,
    "-n",
    testLibName.get,
    "--location",
    testLocation.get
  )

  //missing lane
  val missingOptional = Array(
    Commands.QueryReadGroupBam.apply().commandName,
    "-f",
    testFlowcell.get,
    "-n",
    testLibName.get,
    "--location",
    testLocation.get
  )

  val goodAddCommand = Array(
    Commands.AddReadGroupBam.apply().commandName,
    "-m",
    metadataFileLocation,
    "-f",
    testFlowcell.get,
    "-l",
    "1",
    "-n",
    testLibName.get,
    "--location",
    testLocation.get
  )

}
