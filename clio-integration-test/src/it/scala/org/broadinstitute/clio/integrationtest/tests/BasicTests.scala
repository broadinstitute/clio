package org.broadinstitute.clio.integrationtest.tests

import org.broadinstitute.clio.client.commands.ClioCommand
import org.broadinstitute.clio.integrationtest.{
  BaseIntegrationSpec,
  ClioBuildInfo
}
import org.broadinstitute.clio.status.model.{
  ServerStatusInfo,
  StatusInfo,
  SystemStatusInfo,
  VersionInfo
}
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, StatusCodes}
import akka.http.scaladsl.unmarshalling.Unmarshal
import org.broadinstitute.clio.client.webclient.ClioWebClient

/** Tests of Clio /health and /version endpoints, as well as rejection behaviors. */
trait BasicTests { self: BaseIntegrationSpec =>

  it should "report health information at /health" in {
    runClient(ClioCommand.getServerHealthName)
      .flatMap(Unmarshal(_).to[StatusInfo])
      .map(
        _ should be(StatusInfo(ServerStatusInfo.Started, SystemStatusInfo.OK))
      )
  }

  it should "report the server version of this test at /version" in {
    for {
      response <- runClient(ClioCommand.getServerVersionName)
      versionInfo <- Unmarshal(response).to[VersionInfo]
    } yield {
      versionInfo.version should be(ClioBuildInfo.version)
    }
  }

  it should "reject requests to invalid routes" in {
    recoverToExceptionIf[ClioWebClient.FailedResponse] {
      clioWebClient.dispatchRequest(HttpRequest(uri = "/badpath"))
    }.map {
      _.statusCode should be(StatusCodes.NotFound)
    }
  }

  it should "reject bad HTTP methods to valid routes" in {
    recoverToExceptionIf[ClioWebClient.FailedResponse] {
      clioWebClient
        .dispatchRequest(
          HttpRequest(uri = "/health", method = HttpMethods.POST)
        )
    }.map {
      _.statusCode should be(StatusCodes.MethodNotAllowed)
    }
  }
}
