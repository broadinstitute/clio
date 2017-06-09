package org.broadinstitute.clio.webservice

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import org.broadinstitute.clio.service.AuthService


trait AuthWebService {

  lazy val authRoutes: Route = concat(authorizationRoute)

  val authService = AuthService()

  private[webservice] val authorizationRoute =
    path("authorization") {
      authorize(authService.authorize _) {
        get {
          complete("OK")
        }
      }
    }
}
