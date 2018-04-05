package org.broadinstitute.clio.integrationtest.tests

import akka.http.scaladsl.model.{HttpMethods, HttpRequest, StatusCodes}
import org.broadinstitute.clio.client.commands.ClioCommand
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.integrationtest.{BaseIntegrationSpec, ClioBuildInfo}
import org.broadinstitute.clio.status.model.{StatusInfo, VersionInfo}
import org.broadinstitute.clio.transfer.model.ApiConstants._

/** Tests of Clio /health and /version endpoints, as well as rejection behaviors. */
trait BasicTests { self: BaseIntegrationSpec =>

  it should s"report health information at /$healthString" in {
    runClientGetJsonAs[StatusInfo](ClioCommand.getServerHealthName)
      .map(_ should be(StatusInfo.Running))
  }

  it should s"report the server version of this test at /$versionString" in {
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
          HttpRequest(uri = s"/$healthString", method = HttpMethods.POST),
          includeAuth = false
        )
    }.map {
      _.statusCode should be(StatusCodes.MethodNotAllowed)
    }
  }
}
