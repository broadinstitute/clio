package org.broadinstitute.clio.client.dispatch

import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.commands.AddWgsUbam

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken

class AddWgsUbamSpec extends BaseClientSpec {
  behavior of "AddWgsUbam"

  implicit val bearerToken: OAuth2BearerToken = testBearer

  it should "fail if the metadata is not valid json" in {
    recoverToSucceededIf[Exception] {
      succeedingDispatcher.dispatch(
        AddWgsUbam(
          metadataLocation = badMetadataFileLocation,
          transferWgsUbamV1Key = testTransferV1Key
        ),
      )
    }
  }

  it should "fail if the json is valid but we can't unmarshal it" in {
    recoverToSucceededIf[Exception] {
      succeedingDispatcher.dispatch(
        AddWgsUbam(
          metadataLocation = metadataPlusExtraFieldsFileLocation,
          transferWgsUbamV1Key = testTransferV1Key
        )
      )
    }
  }

  it should "return a failed future if there was a server error" in {
    recoverToSucceededIf[Exception] {
      failingDispatcher
        .dispatch(goodAddCommand)
    }
  }

  it should "return a successful HttpResponse if the server response is OK" in {
    succeedingDispatcher
      .dispatch(goodAddCommand)
      .map(_.status should be(StatusCodes.OK))
  }

}
