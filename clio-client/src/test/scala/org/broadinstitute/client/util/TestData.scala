package org.broadinstitute.client.util

import java.time.OffsetDateTime

import org.broadinstitute.clio.client.commands.Commands

trait TestData {

  val metadataFileLocation = Some("clio-client/src/test/resources/testdata/metadata")
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
  val testRunDateStart: Option[OffsetDateTime] = Some(OffsetDateTime.now())
  val testRunDateEnd: Option[OffsetDateTime] = Some(
    OffsetDateTime.now().plusHours(1)
  )

  //missing lane
  val missingRequired = Array(
    Commands.AddWgsUbam.toString,
    "-m",
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
    Commands.QueryWgsUbam.toString,
    "-f",
    testFlowcell.get,
    "-n",
    testLibName.get,
    "--location",
    testLocation.get
  )

  val goodAddCommand = Array(
    Commands.AddWgsUbam.toString,
    "-m",
    metadataFileLocation.get,
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
