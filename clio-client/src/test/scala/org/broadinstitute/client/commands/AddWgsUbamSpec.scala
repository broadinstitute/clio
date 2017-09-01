package org.broadinstitute.client.commands

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import io.circe.{DecodingFailure, ParsingFailure}
import org.broadinstitute.client.BaseClientSpec
import org.broadinstitute.clio.client.commands.AddWgsUbam

class AddWgsUbamSpec extends BaseClientSpec {
  behavior of "AddReadGroupBam"

  implicit val bearerToken: OAuth2BearerToken = testBearer

  it should "throw a parsing failure if the metadata is not valid json" in {
    recoverToSucceededIf[ParsingFailure] {
      succeedingDispatcher.dispatch(
        AddWgsUbam(
          metadataLocation = badMetadataFileLocation,
          transferWgsUbamV1Key = testTransferV1Key
        ),
      )
    }
  }

  it should "throw a decoding failure if the json is valid but we can't unmarshal it" in {
    recoverToSucceededIf[DecodingFailure] {
      succeedingDispatcher.dispatch(
        AddWgsUbam(
          metadataLocation = metadataPlusExtraFieldsFileLocation,
          transferWgsUbamV1Key = testTransferV1Key
        )
      )
    }
  }

  it should "return a failing HttpResponse if there was a server error" in {
    recoverToSucceededIf[Exception] {
      failingDispatcher
        .dispatch(goodAddCommand)
    }
  }

  it should "return successful HttpResponse if the server response is OK" in {

    succeedingDispatcher
      .dispatch(goodAddCommand)
      .map(_.status should be(StatusCodes.OK))

  }

}
