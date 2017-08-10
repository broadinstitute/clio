package org.broadinstitute.clio.server.webservice

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import org.broadinstitute.clio.server.service.ReadGroupService
import org.broadinstitute.clio.server.webservice.WebServiceAutoDerivation._
import org.broadinstitute.clio.transfer.model._
import org.broadinstitute.clio.util.model.Location

trait ReadGroupWebService {

  def readGroupService: ReadGroupService

  lazy val readGroupRoutes: Route = {
    pathPrefix("v1") {
      pathPrefix("readgroup") {
        concat(getSchema, postMetadata, query, queryall)
      }
    }
  }

  private[webservice] val pathPrefixKey: Directive1[TransferReadGroupV1Key] = {
    for {
      flowcellBarcode <- pathPrefix(Segment)
      lane <- pathPrefix(IntNumber)
      libraryName <- pathPrefix(Segment)
      location <- pathPrefix(Location.pathMatcher)
    } yield TransferReadGroupV1Key(flowcellBarcode, lane, libraryName, location)
  }

  private[webservice] val postMetadata: Route = {
    pathPrefix("metadata") {
      pathPrefixKey { key =>
        post {
          entity(as[TransferReadGroupV1Metadata]) { metadata =>
            complete(readGroupService.upsertMetadata(key, metadata))
          }
        }
      }
    }
  }

  private[webservice] val query: Route = {
    path("query") {
      post {
        entity(as[TransferReadGroupV1QueryInput]) { input =>
          complete(readGroupService.queryMetadata(input))
        }
      }
    }
  }

  private[webservice] val queryall: Route = {
    path("queryall") {
      post {
        entity(as[TransferReadGroupV1QueryInput]) { input =>
          complete(readGroupService.queryAllMetadata(input))
        }
      }
    }
  }

  private[webservice] val getSchema: Route = {
    path("schema") {
      get {
        complete(readGroupService.querySchema())
      }
    }
  }
}
