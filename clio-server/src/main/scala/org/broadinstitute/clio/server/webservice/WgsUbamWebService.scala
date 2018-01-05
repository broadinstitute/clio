package org.broadinstitute.clio.server.webservice

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import org.broadinstitute.clio.server.service.WgsUbamService
import org.broadinstitute.clio.transfer.model.ubam.{
  TransferUbamV1Key,
  TransferUbamV1Metadata,
  TransferUbamV1QueryInput
}
import org.broadinstitute.clio.util.model.Location

trait WgsUbamWebService { self: JsonWebService =>

  def wgsUbamService: WgsUbamService

  lazy val wgsUbamRoutes: Route = {
    pathPrefix("v1") {
      pathPrefix("wgsubam") {
        concat(getSchema, postMetadata, query, queryall)
      }
    }
  }

  private[webservice] val pathPrefixKey: Directive1[TransferUbamV1Key] = {
    for {
      location <- pathPrefix(Location.namesToValuesMap)
      flowcellBarcode <- pathPrefix(Segment)
      lane <- pathPrefix(IntNumber)
      libraryName <- pathPrefix(Segment)
    } yield TransferUbamV1Key(location, flowcellBarcode, lane, libraryName)
  }

  private[webservice] val postMetadata: Route = {
    pathPrefix("metadata") {
      pathPrefixKey { key =>
        post {
          entity(as[TransferUbamV1Metadata]) { metadata =>
            complete(wgsUbamService.upsertMetadata(key, metadata))
          }
        }
      }
    }
  }

  private[webservice] val query: Route = {
    path("query") {
      post {
        entity(as[TransferUbamV1QueryInput]) { input =>
          complete(wgsUbamService.queryMetadata(input))
        }
      }
    }
  }

  private[webservice] val queryall: Route = {
    path("queryall") {
      post {
        entity(as[TransferUbamV1QueryInput]) { input =>
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
