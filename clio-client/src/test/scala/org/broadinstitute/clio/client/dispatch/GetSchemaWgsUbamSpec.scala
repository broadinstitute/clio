package org.broadinstitute.clio.client.dispatch

import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.commands.GetSchemaWgsUbam

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken

class GetSchemaWgsUbamSpec extends BaseClientSpec {
  behavior of "GetSchemaWgsUbam"

  implicit val bearerToken: OAuth2BearerToken = testBearer

  it should "return a failed future if there was a server error" in {
    recoverToSucceededIf[Exception] {
      failingDispatcher.dispatch(GetSchemaWgsUbam)
    }
  }

  it should "return a successful HttpResponse if the server response is OK" in {
    succeedingDispatcher()
      .dispatch(GetSchemaWgsUbam)
      .map(_.status should be(StatusCodes.OK))
  }
}
