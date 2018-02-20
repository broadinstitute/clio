package org.broadinstitute.clio.client.dispatch

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.commands.AddWgsUbam
import org.broadinstitute.clio.client.util.MockIoUtil
import org.broadinstitute.clio.util.model.UpsertId

class AddWgsUbamSpec extends BaseClientSpec {
  behavior of "AddWgsUbam"

  implicit val bearerToken: OAuth2BearerToken = testBearer

  val dispatcher = succeedingDispatcher()

  it should "fail if the metadata is not valid json" in {
    recoverToSucceededIf[IllegalArgumentException] {
      dispatcher.dispatch(
        AddWgsUbam(
          metadataLocation = badMetadataFileLocation,
          key = testTransferV1Key
        ),
      )
    }
  }

  it should "fail if the json is valid but we can't decode it" in {
    recoverToSucceededIf[IllegalArgumentException] {
      dispatcher.dispatch(
        AddWgsUbam(
          metadataLocation = metadataPlusExtraFieldsFileLocation,
          key = testTransferV1Key
        )
      )
    }
  }

  it should "return a failed future if there was a server error" in {
    recoverToSucceededIf[RuntimeException] {
      failingDispatcher
        .dispatch(goodAddCommand)
    }
  }

  it should "return an UpsertId if the server response is OK" in {
    dispatcher
      .dispatch(goodAddCommand)
      .map(_ shouldBe an[UpsertId])
  }

  it should "fail to add a ubam that would overwrite an existing document" in {
    recoverToSucceededIf[IllegalArgumentException] {
      val mockIoUtil = new MockIoUtil
      succeedingDispatcher(mockIoUtil, testWgsUbamLocation)
        .dispatch(goodAddCommand)
    }
  }

  it should "succeed in overwriting an existing document if a force flag is set" in {
    val mockIoUtil = new MockIoUtil
    succeedingDispatcher(mockIoUtil, testWgsChangedUbamLocation)
      .dispatch(goodAddCommand.copy(force = true))
      .map(_ shouldBe an[UpsertId])
  }
}
