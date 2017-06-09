package org.broadinstitute.clio.webservice

import akka.http.scaladsl.server.Directives._
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import org.broadinstitute.clio.service.AuthorizationService


trait AuthorizationWebService {

  val authorizationService = AuthorizationService()

  val authorizationRoute =
    path("authorization") {
      authorize(authorizationService.authorize _) {
        get {
          complete("OK")
        }
      }
    }
}
