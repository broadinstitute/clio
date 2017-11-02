package org.broadinstitute.clio.client.dispatch

import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.commands.GetServerVersion

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken

class GetServerVersionExecutorSpec extends BaseClientSpec {
  behavior of "GetServerVersion"

  implicit val bearerToken: OAuth2BearerToken = testBearer

  it should "return a failed future if there was a server error" in {
    recoverToSucceededIf[Exception] {
      failingDispatcher.dispatch(GetServerVersion)
    }
  }

  it should "return a successful HttpResponse if the server response is OK" in {
    succeedingDispatcher()
      .dispatch(GetServerVersion)
      .map(_.status should be(StatusCodes.OK))
  }
}
