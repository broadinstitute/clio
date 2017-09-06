package org.broadinstitute.clio.integrationtest.tests

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

/** Tests of Clio /health and /version endpoints, as well as rejection behaviors. */
trait BasicTests { self: BaseIntegrationSpec =>

  it should "report health information at /health" in {
    runClient("get-server-health")
      .flatMap(Unmarshal(_).to[StatusInfo])
      .map(
        _ should be(StatusInfo(ServerStatusInfo.Started, SystemStatusInfo.OK))
      )
  }

  it should "report the server version of this test at /version" in {
    for {
      response <- runClient("get-server-version")
      versionInfo <- Unmarshal(response).to[VersionInfo]
    } yield {
      versionInfo.version should be(ClioBuildInfo.version)
    }
  }

  it should "reject requests to invalid routes" in {
    clioWebClient
      .dispatchRequest(HttpRequest(uri = "/badpath"))
      .map(_.status should be(StatusCodes.NotFound))
  }

  it should "reject bad HTTP methods to valid routes" in {
    clioWebClient
      .dispatchRequest(HttpRequest(uri = "/health", method = HttpMethods.POST))
      .map(_.status should be(StatusCodes.MethodNotAllowed))
  }
}
