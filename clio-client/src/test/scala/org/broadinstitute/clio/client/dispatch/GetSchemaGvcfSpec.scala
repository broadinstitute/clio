package org.broadinstitute.clio.client.dispatch

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.commands.GetSchemaGvcf
import org.broadinstitute.clio.transfer.model.GvcfIndex

class GetSchemaGvcfSpec extends BaseClientSpec {
  behavior of "GetSchemaGvcf"

  implicit val bearerToken: OAuth2BearerToken = testBearer

  it should "return a failed future if there was a server error" in {
    recoverToSucceededIf[Exception] {
      failingDispatcher.dispatch(GetSchemaGvcf)
    }
  }

  it should "return the gvcf schema as JSON" in {
    succeedingDispatcher()
      .dispatch(GetSchemaGvcf)
      .map(_ should be(GvcfIndex.jsonSchema))
  }
}
