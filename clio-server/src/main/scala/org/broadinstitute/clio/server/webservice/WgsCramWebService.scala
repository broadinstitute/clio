package org.broadinstitute.clio.server.webservice

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import org.broadinstitute.clio.server.service.WgsCramService
import org.broadinstitute.clio.server.webservice.WebServiceAutoDerivation._
import org.broadinstitute.clio.transfer.model.wgscram.{TransferWgsCramV1Key, TransferWgsCramV1Metadata, TransferWgsCramV1QueryInput}
import org.broadinstitute.clio.util.model.Location

trait WgsCramWebService {

  def wgsCramService: WgsCramService

  lazy val wgsCramRoutes: Route = {
    pathPrefix("v1") {
      pathPrefix("wgscram") {
        concat(getSchema, postMetadata, query, queryall)
      }
    }
  }

  private[webservice] val pathPrefixKey: Directive1[TransferWgsCramV1Key] = {
    for {
      location <- pathPrefix(Location.pathMatcher)
      project <- pathPrefix(Segment)
      sampleAlias <- pathPrefix(Segment)
      versoion <- pathPrefix(IntNumber)
    } yield TransferWgsCramV1Key(location, project, sampleAlias, versoion)
  }

  private[webservice] val postMetadata: Route = {
    pathPrefix("metadata") {
      pathPrefixKey { key =>
        post {
          entity(as[TransferWgsCramV1Metadata]) { metadata =>
            complete(wgsCramService.upsertMetadata(key, metadata))
          }
        }
      }
    }
  }

  private[webservice] val query: Route = {
    path("query") {
      post {
        entity(as[TransferWgsCramV1QueryInput]) { input =>
          complete(wgsCramService.queryMetadata(input))
        }
      }
    }
  }

  private[webservice] val queryall: Route = {
    path("queryall") {
      post {
        entity(as[TransferWgsCramV1QueryInput]) { input =>
          complete(wgsCramService.queryAllMetadata(input))
        }
      }
    }
  }

  private[webservice] val getSchema: Route = {
    path("schema") {
      get {
        complete(wgsCramService.querySchema())
      }
    }
  }
}
