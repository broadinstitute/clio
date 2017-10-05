package org.broadinstitute.clio.server.webservice

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import org.broadinstitute.clio.server.service.StatusService

trait StatusWebService { self: JsonWebService =>
  lazy val statusRoutes: Route =
    concat(versionRoute, healthRoute, slashRedirectRoute)

  def statusService: StatusService

  private[webservice] val versionRoute =
    path("version") {
      get {
        complete(statusService.getVersion)
      }
    }

  private[webservice] val healthRoute = {
    path("health") {
      get {
        complete(statusService.getStatus)
      }
    }
  }
  private[webservice] val slashRedirectRoute = {
    pathSingleSlash {
      redirect("/health", StatusCodes.PermanentRedirect)
    }
  }
}
