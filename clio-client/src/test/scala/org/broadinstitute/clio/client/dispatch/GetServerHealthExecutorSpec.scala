package org.broadinstitute.clio.client.dispatch

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import io.circe.Json
import org.broadinstitute.clio.client.BaseClientSpec
import org.broadinstitute.clio.client.commands.GetServerHealth
import org.broadinstitute.clio.status.model.{ClioStatus, StatusInfo, SearchStatus}

class GetServerHealthExecutorSpec extends BaseClientSpec {
  behavior of "GetServerHealth"

  implicit val bearerToken: OAuth2BearerToken = testBearer

  it should "return a failed future if there was a server error" in {
    recoverToSucceededIf[Exception] {
      failingDispatcher.dispatch(GetServerHealth)
    }
  }

  it should "return the server status as JSON if the server response is OK" in {
    succeedingDispatcher()
      .dispatch(GetServerHealth)
      .map {
        case json: Json =>
          json.as[StatusInfo] should be(
            Right(StatusInfo(ClioStatus.Started, SearchStatus.OK))
          )
        case other => fail(s"Expected json, got $other")
      }
  }
}
