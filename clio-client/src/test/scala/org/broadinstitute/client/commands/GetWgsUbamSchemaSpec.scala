package org.broadinstitute.client.commands

import org.broadinstitute.client.BaseClientSpec
import org.broadinstitute.clio.client.commands.GetWgsUbamSchema

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken

class GetWgsUbamSchemaSpec extends BaseClientSpec {
  behavior of "GetWgsUbamSchema"

  implicit val bearerToken: OAuth2BearerToken = testBearer

  it should "return a failed future if there was a server error" in {
    recoverToSucceededIf[Exception] {
      failingDispatcher.dispatch(GetWgsUbamSchema)
    }
  }

  it should "return a successful HttpResponse if the server response is OK" in {
    succeedingDispatcher
      .dispatch(GetWgsUbamSchema)
      .map(_.status should be(StatusCodes.OK))
  }
}
