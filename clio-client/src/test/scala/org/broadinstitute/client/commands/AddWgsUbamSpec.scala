package org.broadinstitute.client.commands

import io.circe.{DecodingFailure, ParsingFailure}
import org.broadinstitute.client.BaseClientSpec
import org.broadinstitute.clio.client.commands.Commands.AddWgsUbam
import org.broadinstitute.clio.client.parser.BaseArgs

class AddWgsUbamSpec extends BaseClientSpec {
  behavior of "AddWgsUBam"

  it should "throw a parsing failure if the metadata is not valid json" in {
    recoverToSucceededIf[ParsingFailure] {
      val config = BaseArgs(
        command = Some(AddWgsUbam),
        flowcell = testFlowcell,
        lane = testLane,
        libraryName = testLibName,
        location = testLocation,
        metadataLocation = badMetadataFileLocation,
        bearerToken = testBearer
      )
      succeedingDispatcher.dispatch(config)
    }
  }

  it should "throw a decoding failure if the json is valid but we can't unmarshal it" in {
    recoverToSucceededIf[DecodingFailure] {
      val config = BaseArgs(
        command = Some(AddWgsUbam),
        flowcell = testFlowcell,
        lane = testLane,
        libraryName = testLibName,
        location = testLocation,
        metadataLocation = metadataPlusExtraFieldsFileLocation,
        bearerToken = testBearer
      )
      succeedingDispatcher.dispatch(config)
    }
  }

  it should "return false if there was a server error" in {
    val config = BaseArgs(
      command = Some(AddWgsUbam),
      flowcell = testFlowcell,
      lane = testLane,
      libraryName = testLibName,
      location = testLocation,
      metadataLocation = metadataFileLocation,
      bearerToken = testBearer
    )
    failingDispatcher
      .dispatch(config)
      .map(_ should be(false))
  }

  it should "return true if the server response is OK" in {
    val config = BaseArgs(
      command = Some(AddWgsUbam),
      flowcell = testFlowcell,
      lane = testLane,
      libraryName = testLibName,
      location = testLocation,
      metadataLocation = metadataFileLocation,
      bearerToken = testBearer
    )
    succeedingDispatcher
      .dispatch(config)
      .map(_ should be(true))
  }

}
