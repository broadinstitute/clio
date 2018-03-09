package org.broadinstitute.clio.client.dispatch

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.commands.GetSchemaWgsUbam
import org.broadinstitute.clio.transfer.model.WgsUbamIndex

class GetSchemaWgsUbamSpec extends BaseClientSpec {
  behavior of "GetSchemaWgsUbam"

  implicit val bearerToken: OAuth2BearerToken = testBearer

  it should "return a failed future if there was a server error" in {
    recoverToSucceededIf[Exception] {
      failingDispatcher.dispatch(GetSchemaWgsUbam)
    }
  }

  it should "return the ubam schema as JSON" in {
    succeedingDispatcher()
      .dispatch(GetSchemaWgsUbam)
      .map(_ should be(WgsUbamIndex.jsonSchema))
  }
}
