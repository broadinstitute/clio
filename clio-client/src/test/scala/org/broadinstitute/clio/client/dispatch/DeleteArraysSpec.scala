package org.broadinstitute.clio.client.dispatch

import java.net.URI

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.commands.DeleteArrays
import org.broadinstitute.clio.client.util.MockIoUtil
import org.broadinstitute.clio.client.webclient.MockClioWebClient

/**
  * FIXME: These are mostly bogus now because the server does not
  * implement the Arrays index yet.
  */
class DeleteArraysSpec extends BaseClientSpec {
  behavior of "DeleteArrays"

  implicit val bearToken: OAuth2BearerToken = testBearer

  it should "throw an exception if the location is not GCP" in {
    recoverToSucceededIf[UnsupportedOperationException] {
      val command = DeleteArrays(
        key = testArraysKey,
        note = "Deleting for arrays test"
      )
      succeedingDispatcher().dispatch(command)
    }
  }

  it should "throw an exception if Clio returns an error" in {
    recoverToSucceededIf[RuntimeException] {
      failingDispatcher.dispatch(goodArraysDeleteCommand)
    }
  }

  it should "throw an exception if Clio can't delete the cloud file" in {
    recoverToSucceededIf[RuntimeException] {
      succeedingDispatcher(
        new MockIoUtil {
          override def deleteGoogleObject(path: URI): Int = 1
          override def googleObjectExists(path: URI): Boolean = true
        },
        testArraysLocation
      ).dispatch(goodArraysDeleteCommand)
    }
  }

  it should "throw an exception if Clio can't delete the arrays record" in {
    val mockIoUtil = new MockIoUtil
    mockIoUtil.putFileInCloud(testArraysCloudSourcePath)
    recoverToSucceededIf[RuntimeException] {
      new CommandDispatch(MockClioWebClient.failingToUpsert, mockIoUtil)
        .dispatch(goodArraysDeleteCommand)
    }
  }
}
