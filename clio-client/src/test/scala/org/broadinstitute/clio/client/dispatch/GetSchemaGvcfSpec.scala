package org.broadinstitute.clio.client.dispatch

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.commands.GetSchemaGvcf

class GetSchemaGvcfSpec extends BaseClientSpec {
  behavior of "GetSchemaGvcf"

  implicit val bearerToken: OAuth2BearerToken = testBearer

  it should "return a failed future if there was a server error" in {
    recoverToSucceededIf[Exception] {
      failingDispatcherGvcf.dispatch(GetSchemaGvcf)
    }
  }

  it should "return a successful HttpResponse if the server response is OK" in {
    succeedingDispatcher
      .dispatch(GetSchemaGvcf)
      .map(_.status should be(StatusCodes.OK))
  }
}
