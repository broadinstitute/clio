package org.broadinstitute.clio.client.dispatch

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.commands.AddGvcf
import org.broadinstitute.clio.client.util.MockIoUtil
import org.broadinstitute.clio.util.model.UpsertId

class AddGvcfSpec extends BaseClientSpec {
  behavior of "AddGvcf"

  implicit val bearerToken: OAuth2BearerToken = testBearer

  val dispatcher = succeedingDispatcher()

  it should "fail if the metadata is not valid json" in {
    recoverToSucceededIf[IllegalArgumentException] {
      dispatcher.dispatch(
        AddGvcf(
          metadataLocation = badMetadataFileLocation,
          key = testGvcfKey
        )
      )
    }
  }

  it should "fail if the json is valid but we can't decode it" in {
    recoverToSucceededIf[IllegalArgumentException] {
      dispatcher.dispatch(
        AddGvcf(
          metadataLocation = metadataPlusExtraFieldsFileLocation,
          key = testGvcfKey
        )
      )
    }
  }

  it should "return a failed future if there was a server error" in {
    recoverToSucceededIf[RuntimeException] {
      failingDispatcher
        .dispatch(goodGvcfAddCommand)
    }
  }

  it should "return an UpsertId if the server response is OK" in {
    dispatcher
      .dispatch(goodGvcfAddCommand)
      .map(_ shouldBe an[UpsertId])
  }

  it should "succeed in overwriting an existing document if a force flag is set" in {
    val mockIoUtil = new MockIoUtil
    succeedingDispatcher(mockIoUtil, testGvcfLocation)
      .dispatch(goodGvcfAddCommand.copy(force = true))
      .map(_ shouldBe an[UpsertId])
  }
}
