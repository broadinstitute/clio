package org.broadinstitute.clio.client.dispatch

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.commands.AddGvcf

class AddGvcfSpec extends BaseClientSpec {
  behavior of "AddGvcf"

  implicit val bearerToken: OAuth2BearerToken = testBearer

  val dispatcher = succeedingDispatcher()

  it should "fail if the metadata is not valid json" in {
    recoverToSucceededIf[Exception] {
      dispatcher.dispatch(
        AddGvcf(
          metadataLocation = badMetadataFileLocation,
          key = testGvcfTransferV1Key
        )
      )
    }
  }

  it should "fail if the json is valid but we can't unmarshal it" in {
    recoverToSucceededIf[Exception] {
      dispatcher.dispatch(
        AddGvcf(
          metadataLocation = metadataPlusExtraFieldsFileLocation,
          key = testGvcfTransferV1Key
        )
      )
    }
  }

  it should "return a failed future if there was a server error" in {
    recoverToSucceededIf[Exception] {
      failingDispatcher
        .dispatch(goodGvcfAddCommand)
    }
  }

  it should "return a successful HttpResponse if the server response is OK" in {
    dispatcher
      .dispatch(goodGvcfAddCommand)
      .map(_.status should be(StatusCodes.OK))
  }

}
