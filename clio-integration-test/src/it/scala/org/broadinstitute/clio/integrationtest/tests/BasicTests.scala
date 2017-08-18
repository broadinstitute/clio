package org.broadinstitute.clio.integrationtest.tests

import org.broadinstitute.clio.integrationtest.BaseIntegrationSpec
import org.broadinstitute.clio.status.model.{ServerStatusInfo, StatusInfo, SystemStatusInfo, VersionInfo}

import akka.http.scaladsl.model.{HttpMethods, HttpRequest, StatusCodes}
import akka.http.scaladsl.unmarshalling.Unmarshal

/** Tests of Clio /health and /version endpoints, as well as rejection behaviors. */
trait BasicTests { self: BaseIntegrationSpec =>

  it should "report health information at /health" in {
    clioWebClient.getClioServerHealth
      .flatMap(Unmarshal(_).to[StatusInfo])
      .map(
        _ should be(StatusInfo(ServerStatusInfo.Started, SystemStatusInfo.OK))
      )
  }

  it should "report a git-hash version at /version" in {
    clioWebClient.getClioServerVersion
      .flatMap(Unmarshal(_).to[VersionInfo])
      .map(
        _.version should fullyMatch regex """\d+\.\d+\.\d+-g[0-9a-f]{7}-SNAP"""
      )
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
