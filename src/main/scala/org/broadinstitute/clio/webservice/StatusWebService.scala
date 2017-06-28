package org.broadinstitute.clio.webservice

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import io.circe.generic.auto._
import org.broadinstitute.clio.service.StatusService

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
