package org.broadinstitute.client.commands

import io.circe.{DecodingFailure, ParsingFailure}
import org.broadinstitute.client.BaseClientSpec
import org.broadinstitute.client.webclient.MockClioWebClient
import org.broadinstitute.clio.client.commands.{AddWgsUbam, CommandDispatch}

class AddWgsUbamSpec extends BaseClientSpec {
  behavior of "AddReadGroupBam"

  it should "throw a parsing failure if the metadata is not valid json" in {

    a[ParsingFailure] should be thrownBy {
      CommandDispatch.checkResponse(
        AddWgsUbam(
          metadataLocation = badMetadataFileLocation,
          transferWgsUbamV1Key = testTransferV1Key
        ).execute(MockClioWebClient.returningOk, testBearer)
      )
    }
  }

  it should "throw a decoding failure if the json is valid but we can't unmarshal it" in {
    a[DecodingFailure] should be thrownBy {
      CommandDispatch.checkResponse(
        AddWgsUbam(
          metadataLocation = metadataPlusExtraFieldsFileLocation,
          transferWgsUbamV1Key = testTransferV1Key
        ).execute(MockClioWebClient.returningOk, testBearer)
      )
    }
  }

  it should "return false if there was a server error" in {
    CommandDispatch.checkResponse(
      AddWgsUbam(
        metadataLocation = metadataFileLocation,
        transferWgsUbamV1Key = testTransferV1Key
      ).execute(MockClioWebClient.returningInternalError, testBearer)
    ) should be(false)
  }

  it should "return true if the server response is OK" in {
    CommandDispatch.checkResponse(
      AddWgsUbam(
        metadataLocation = metadataFileLocation,
        transferWgsUbamV1Key = testTransferV1Key
      ).execute(MockClioWebClient.returningOk, testBearer)
    ) should be(true)
  }

}
