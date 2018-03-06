package org.broadinstitute.clio.client.dispatch

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.commands.MoveArrays
import org.broadinstitute.clio.client.util.MockIoUtil
import org.broadinstitute.clio.client.webclient.MockClioWebClient

class MoveArraysSpec extends BaseClientSpec {
  behavior of "MoveArrays"

  implicit val bearerToken: OAuth2BearerToken = testBearer

  it should "throw an exception if an arrays file does not exist" in {
    recoverToSucceededIf[RuntimeException] {
      val command =
        MoveArrays(
          key = testArraysTransferV1Key,
          destination = testArraysCloudSourcePath
        )
      succeedingDispatcher().dispatch(command)
    }
  }

  /**
    * FIXME: Bogus
    */
  it should "throw an exception if Clio can't upsert the new arrays record" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testArraysCloudSourcePath)
    recoverToSucceededIf[RuntimeException] {
      new CommandDispatch(MockClioWebClient.failingToUpsert, mockIoUtil)
        .dispatch(goodArraysMoveCommand)
    }
  }

  /**
    * FIXME: Bogus because UnsupportedOperationException is due to no Arrays implementation in the server.
    */
  it should "throw an exception if given non-GCP arrays metadata" in {
    recoverToSucceededIf[UnsupportedOperationException] {
      val command = MoveArrays(
        key = testArraysTransferV1Key,
        destination = testCloudDestinationDirectoryPath
      )
      succeedingDispatcher().dispatch(command)
    }
  }
}
