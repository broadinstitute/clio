package org.broadinstitute.clio.integrationtest.tests

import akka.http.scaladsl.model.{HttpMethods, HttpRequest, StatusCodes}
import org.broadinstitute.clio.client.commands.ClioCommand
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.integrationtest.{BaseIntegrationSpec, ClioBuildInfo}
import org.broadinstitute.clio.status.model.{
  ServerStatusInfo,
  StatusInfo,
  SystemStatusInfo,
  VersionInfo
}

/** Tests of Clio /health and /version endpoints, as well as rejection behaviors. */
trait BasicTests { self: BaseIntegrationSpec =>

  it should "report health information at /health" in {
    runClientGetJsonAs[StatusInfo](ClioCommand.getServerHealthName)
      .map(
        _ should be(StatusInfo(ServerStatusInfo.Started, SystemStatusInfo.OK))
      )
  }

  it should "report the server version of this test at /version" in {
    runClientGetJsonAs[VersionInfo](ClioCommand.getServerVersionName)
      .map(_ should be(VersionInfo(ClioBuildInfo.version)))
  }

  it should "reject requests to invalid routes" in {
    recoverToExceptionIf[ClioWebClient.FailedResponse] {
      clioWebClient.dispatchRequest(
        HttpRequest(uri = "/badpath"),
        includeAuth = false
      )
    }.map {
      _.statusCode should be(StatusCodes.NotFound)
    }
  }

  it should "reject bad HTTP methods to valid routes" in {
    recoverToExceptionIf[ClioWebClient.FailedResponse] {
      clioWebClient
        .dispatchRequest(
          HttpRequest(uri = "/health", method = HttpMethods.POST),
          includeAuth = false
        )
    }.map {
      _.statusCode should be(StatusCodes.MethodNotAllowed)
    }
  }
}
