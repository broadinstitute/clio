package org.broadinstitute.clio.client.dispatch

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.commands.AddArrays

class AddArraysSpec extends BaseClientSpec {
  behavior of "AddArrays"

  implicit val bearerToken: OAuth2BearerToken = testBearer

  val dispatcher = succeedingDispatcher()

  it should "fail if the metadata is not valid json" in {
    recoverToSucceededIf[IllegalArgumentException] {
      dispatcher.dispatch(
        AddArrays(
          metadataLocation = badMetadataFileLocation,
          key = testArraysKey
        )
      )
    }
  }

  it should "fail if the json is valid but we can't decode it" in {
    recoverToSucceededIf[IllegalArgumentException] {
      dispatcher.dispatch(
        AddArrays(
          metadataLocation = metadataPlusExtraFieldsFileLocation,
          key = testArraysKey
        )
      )
    }
  }

  /**
    * FIXME: Bogus because server does not support Arrays yet.
    */
  it should "return a failed future if there was a server error" in {
    recoverToSucceededIf[RuntimeException] {
      failingDispatcher
        .dispatch(goodArraysAddCommand)
    }
  }
}
