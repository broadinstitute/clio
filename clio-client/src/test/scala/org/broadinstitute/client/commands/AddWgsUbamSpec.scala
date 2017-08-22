package org.broadinstitute.client.commands

import org.broadinstitute.client.BaseClientSpec
import org.broadinstitute.client.webclient.MockClioWebClient

import io.circe.{DecodingFailure, ParsingFailure}
import org.broadinstitute.clio.client.commands.AddWgsUbam

class AddWgsUbamSpec extends BaseClientSpec {
  behavior of "AddWgsUbam"

  it should "throw a parsing failure if the metadata is not valid json" in {
    a[ParsingFailure] should be thrownBy {
      new AddWgsUbam(
        clioWebClient = MockClioWebClient.returningOk,
        flowcell = testFlowcell,
        lane = testLane,
        libraryName = testLibName,
        location = testLocation,
        metadataLocation = badMetadataFileLocation,
        bearerToken = testBearer
      ).execute
    }
  }

  it should "throw a decoding failure if the json is valid but we can't unmarshal it" in {
    a[DecodingFailure] should be thrownBy {
      new AddWgsUbam(
        clioWebClient = MockClioWebClient.returningOk,
        flowcell = testFlowcell,
        lane = testLane,
        libraryName = testLibName,
        location = testLocation,
        metadataLocation = metadataPlusExtraFieldsFileLocation,
        bearerToken = testBearer
      ).execute
    }
  }

  it should "return false if there was a server error" in {
    new AddWgsUbam(
      clioWebClient = MockClioWebClient.returningInternalError,
      flowcell = testFlowcell,
      lane = testLane,
      libraryName = testLibName,
      location = testLocation,
      metadataLocation = metadataFileLocation,
      bearerToken = testBearer
    ).execute should be(false)
  }

  it should "return true if the server response is OK" in {
    new AddWgsUbam(
      clioWebClient = MockClioWebClient.returningOk,
      flowcell = testFlowcell,
      lane = testLane,
      libraryName = testLibName,
      location = testLocation,
      metadataLocation = metadataFileLocation,
      bearerToken = testBearer
    ).execute should be(true)
  }

}
