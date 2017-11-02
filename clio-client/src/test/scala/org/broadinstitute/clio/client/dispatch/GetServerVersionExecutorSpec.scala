package org.broadinstitute.clio.client.dispatch

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import io.circe.Json
import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.commands.GetServerVersion
import org.broadinstitute.clio.status.model.VersionInfo

class GetServerVersionExecutorSpec extends BaseClientSpec {
  behavior of "GetServerVersion"

  implicit val bearerToken: OAuth2BearerToken = testBearer

  it should "return a failed future if there was a server error" in {
    recoverToSucceededIf[Exception] {
      failingDispatcher.dispatch(GetServerVersion)
    }
  }

  it should "return version info as JSON if the server response is OK" in {
    succeedingDispatcher()
      .dispatch(GetServerVersion)
      .map {
        case json: Json =>
          json.as[VersionInfo] should be(Right(VersionInfo("0.0.1")))
        case other => fail(s"Expected json, got $other")
      }
  }
}
