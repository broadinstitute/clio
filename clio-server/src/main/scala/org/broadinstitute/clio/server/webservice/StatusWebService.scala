package org.broadinstitute.clio.server.webservice

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import org.broadinstitute.clio.server.service.StatusService
import org.broadinstitute.clio.transfer.model.ApiConstants._

class StatusWebService(statusService: StatusService) extends JsonWebService {
  lazy val statusRoutes: Route =
    concat(versionRoute, healthRoute, slashRedirectRoute)

  private[webservice] val versionRoute =
    path(versionString) {
      get {
        complete(statusService.getVersion)
      }
    }

  private[webservice] val healthRoute = {
    path(healthString) {
      get {
        complete(statusService.getStatus)
      }
    }
  }
  private[webservice] val slashRedirectRoute = {
    pathSingleSlash {
      redirect(healthString, StatusCodes.PermanentRedirect)
    }
  }
}
