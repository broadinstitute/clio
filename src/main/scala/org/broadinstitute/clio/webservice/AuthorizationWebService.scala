package org.broadinstitute.clio.webservice

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import org.broadinstitute.clio.service.AuthorizationService


trait AuthorizationWebService {

  lazy val authorizationRoutes: Route = concat(authorizationRoute)

  val authorizationService = AuthorizationService()

  private[webservice] val authorizationRoute =
    path("authorization") {
      authorize(authorizationService.authorize _) {
        get {
          complete("OK")
        }
      }
    }
}
