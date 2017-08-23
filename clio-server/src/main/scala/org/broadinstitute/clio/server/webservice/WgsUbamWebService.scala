package org.broadinstitute.clio.server.webservice

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import org.broadinstitute.clio.server.service.WgsUbamService
import org.broadinstitute.clio.server.webservice.WebServiceAutoDerivation._
import org.broadinstitute.clio.transfer.model._
import org.broadinstitute.clio.util.model.Location

trait WgsUbamWebService {

  def wgsUbamService: WgsUbamService

  lazy val wgsUbamRoutes: Route = {
    pathPrefix("v1") {
      pathPrefix("wgsubam") {
        concat(getSchema, postMetadata, query, queryall)
      }
    }
  }

  private[webservice] val pathPrefixKey: Directive1[TransferWgsUbamV1Key] = {
    for {
      flowcellBarcode <- pathPrefix(Segment)
      lane <- pathPrefix(IntNumber)
      libraryName <- pathPrefix(Segment)
      location <- pathPrefix(Location.pathMatcher)
    } yield TransferWgsUbamV1Key(flowcellBarcode, lane, libraryName, location)
  }

  private[webservice] val postMetadata: Route = {
    pathPrefix("metadata") {
      pathPrefixKey { key =>
        post {
          entity(as[TransferWgsUbamV1Metadata]) { metadata =>
            complete(wgsUbamService.upsertMetadata(key, metadata))
          }
        }
      }
    }
  }

  private[webservice] val query: Route = {
    path("query") {
      post {
        entity(as[TransferWgsUbamV1QueryInput]) { input =>
          complete(wgsUbamService.queryMetadata(input))
        }
      }
    }
  }

  private[webservice] val queryall: Route = {
    path("queryall") {
      post {
        entity(as[TransferWgsUbamV1QueryInput]) { input =>
          complete(wgsUbamService.queryAllMetadata(input))
        }
      }
    }
  }

  private[webservice] val getSchema: Route = {
    path("schema") {
      get {
        complete(wgsUbamService.querySchema())
      }
    }
  }
}