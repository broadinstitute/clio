package org.broadinstitute.clio.client.dispatch

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.commands.GetSchemaArrays
import org.broadinstitute.clio.transfer.model.ArraysIndex

class GetSchemaArraysSpec extends BaseClientSpec {
  behavior of "GetSchemaArrays"

  implicit val bearerToken: OAuth2BearerToken = testBearer

  /**
    * FIXME: Bogus because the server does not implement Arrays yet.
    */
  it should "return a failed future if there was a server error" in {
    recoverToSucceededIf[Exception] {
      failingDispatcher.dispatch(GetSchemaArrays)
    }
  }

  it should "return the Arrays schema as JSON" in {
    succeedingDispatcher()
      .dispatch(GetSchemaArrays)
      .map(_ should be(ArraysIndex.jsonSchema))
  }
}
