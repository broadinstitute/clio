package org.broadinstitute.clio.client.dispatch

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.commands.AddGvcf

class AddGvcfSpec extends BaseClientSpec {
  behavior of "AddGvcf"

  implicit val bearerToken: OAuth2BearerToken = testBearer

  it should "fail if the metadata is not valid json" in {
    recoverToSucceededIf[Exception] {
      succeedingDispatcher.dispatch(
        AddGvcf(
          metadataLocation = badMetadataFileLocation,
          transferGvcfV1Key = testGvcfTransferV1Key
        ),
      )
    }
  }

  it should "fail if the json is valid but we can't unmarshal it" in {
    recoverToSucceededIf[Exception] {
      succeedingDispatcher.dispatch(
        AddGvcf(
          metadataLocation = metadataPlusExtraFieldsFileLocation,
          transferGvcfV1Key = testGvcfTransferV1Key
        )
      )
    }
  }

  it should "return a failed future if there was a server error" in {
    recoverToSucceededIf[Exception] {
      failingDispatcherGvcf
        .dispatch(goodGvcfAddCommand)
    }
  }

  it should "return a successful HttpResponse if the server response is OK" in {
    succeedingDispatcher
      .dispatch(goodGvcfAddCommand)
      .map(_.status should be(StatusCodes.OK))
  }

}
