package org.broadinstitute.clio.integrationtest.tests

import akka.http.scaladsl.model.{HttpMethods, HttpRequest, StatusCodes}
import akka.stream.scaladsl.Sink
import org.broadinstitute.clio.client.commands.ClioCommand
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.integrationtest.{BaseIntegrationSpec, ClioBuildInfo}
import org.broadinstitute.clio.status.model.{ClioStatus, StatusInfo, VersionInfo}
import org.broadinstitute.clio.transfer.model.ApiConstants

/** Tests of Clio /health and /version endpoints, as well as rejection behaviors. */
trait BasicTests { self: BaseIntegrationSpec =>

  import ApiConstants._

  it should s"report health information at /$healthString" in {
    /* Elasticsearch sometimes get stuck in "yellow" state in Docker tests, and
     * Clio (probably incorrectly) reports that as an error, so we only check Clio
     * status here to avoid spurious failures.
     *
     * If Elasticsearch is really in an error state, we'll see a bunch of other tests fail.
     */
    runDecode[StatusInfo](ClioCommand.getServerHealthName)
      .map(_.clio should be(ClioStatus.Started))
  }

  it should s"report the server version of this test at /$versionString" in {
    runDecode[VersionInfo](ClioCommand.getServerVersionName)
      .map(_ should be(VersionInfo(ClioBuildInfo.version)))
  }

  it should "reject requests to invalid routes" in {
    recoverToExceptionIf[ClioWebClient.FailedResponse] {
      clioWebClient
        .dispatchRequest(
          HttpRequest(uri = "/badpath"),
          includeAuth = false
        )
        .runWith(Sink.head)
    }.map {
      _.statusCode should be(StatusCodes.NotFound)
    }
  }

  it should "reject bad HTTP methods to valid routes" in {
    recoverToExceptionIf[ClioWebClient.FailedResponse] {
      clioWebClient
        .dispatchRequest(
          HttpRequest(uri = s"/$healthString", method = HttpMethods.POST),
          includeAuth = false
        )
        .runWith(Sink.head)
    }.map {
      _.statusCode should be(StatusCodes.MethodNotAllowed)
    }
  }
}
