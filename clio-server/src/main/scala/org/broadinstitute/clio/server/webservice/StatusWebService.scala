package org.broadinstitute.clio.server.webservice

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import org.broadinstitute.clio.server.service.StatusService
import org.broadinstitute.clio.server.webservice.WebServiceAutoDerivation._

trait StatusWebService {
  lazy val statusRoutes: Route = concat(versionRoute, healthRoute)

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
}
