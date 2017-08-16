package org.broadinstitute.client.commands

import org.broadinstitute.client.BaseClientSpec
import org.broadinstitute.client.webclient.MockClioWebClient

import io.circe.{DecodingFailure, ParsingFailure}
import org.broadinstitute.clio.client.commands.Commands
import org.broadinstitute.clio.client.parser.BaseArgs

class AddReadGroupBamSpec extends BaseClientSpec {
  behavior of "AddReadGroupBam"

  it should "throw a parsing failure if the metadata is not valid json" in {
    a[ParsingFailure] should be thrownBy {
      val config = BaseArgs(
        flowcell = testFlowcell,
        lane = testLane,
        libraryName = testLibName,
        location = testLocation,
        metadataLocation = badMetadataFileLocation,
        bearerToken = testBearer
      )
      Commands.AddReadGroupBam
        .apply()
        .execute(MockClioWebClient.returningOk, config)
    }
  }

  it should "throw a decoding failure if the json is valid but we can't unmarshal it" in {
    a[DecodingFailure] should be thrownBy {
      val config = BaseArgs(
        flowcell = testFlowcell,
        lane = testLane,
        libraryName = testLibName,
        location = testLocation,
        metadataLocation = metadataPlusExtraFieldsFileLocation,
        bearerToken = testBearer
      )
      Commands.AddReadGroupBam
        .apply()
        .execute(MockClioWebClient.returningOk, config)
    }
  }

  it should "return false if there was a server error" in {
    val config = BaseArgs(
      flowcell = testFlowcell,
      lane = testLane,
      libraryName = testLibName,
      location = testLocation,
      metadataLocation = metadataFileLocation,
      bearerToken = testBearer
    )
    Commands.AddReadGroupBam
      .apply()
      .execute(MockClioWebClient.returningInternalError, config) should be(
      false
    )
  }

  it should "return true if the server response is OK" in {
    val config = BaseArgs(
      flowcell = testFlowcell,
      lane = testLane,
      libraryName = testLibName,
      location = testLocation,
      metadataLocation = metadataFileLocation,
      bearerToken = testBearer
    )
    Commands.AddReadGroupBam
      .apply()
      .execute(MockClioWebClient.returningOk, config) should be(true)
  }

}
