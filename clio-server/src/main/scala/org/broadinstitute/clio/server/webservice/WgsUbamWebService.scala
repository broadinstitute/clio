package org.broadinstitute.clio.server.webservice

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import org.broadinstitute.clio.server.service.IndexService
import org.broadinstitute.clio.transfer.model.{UbamIndex, WgsUbamIndex}

class WgsUbamWebService(ubamService: IndexService[UbamIndex.type])
    extends UbamWebService(ubamService) {

  override lazy val routes: Route = {
    pathPrefix("v1") {
      pathPrefix(WgsUbamIndex.urlSegment) {
        concat(postMetadata, query, queryall)
      }
    }
  }
}
