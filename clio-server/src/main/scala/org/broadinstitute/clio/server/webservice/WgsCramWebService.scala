package org.broadinstitute.clio.server.webservice

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import org.broadinstitute.clio.server.service.WgsCramService
import org.broadinstitute.clio.transfer.model.wgscram.{
  TransferWgsCramV1Key,
  TransferWgsCramV1Metadata,
  TransferWgsCramV1QueryInput
}
import org.broadinstitute.clio.util.model.Location

trait WgsCramWebService { self: JsonWebService =>

  def wgsCramService: WgsCramService

  lazy val wgsCramRoutes: Route = {
    pathPrefix("v1") {
      pathPrefix("wgscram") {
        concat(cramGetSchema, cramPostMetadata, cramQuery, cramQueryall)
      }
    }
  }

  private[webservice] val cramPathPrefixKey
    : Directive1[TransferWgsCramV1Key] = {
    for {
      location <- pathPrefix(Location.namesToValuesMap)
      project <- pathPrefix(Segment)
      sampleAlias <- pathPrefix(Segment)
      version <- pathPrefix(IntNumber)
    } yield TransferWgsCramV1Key(location, project, sampleAlias, version)
  }

  private[webservice] val cramPostMetadata: Route = {
    pathPrefix("metadata") {
      cramPathPrefixKey { key =>
        post {
          entity(as[TransferWgsCramV1Metadata]) { metadata =>
            complete(wgsCramService.upsertMetadata(key, metadata))
          }
        }
      }
    }
  }

  private[webservice] val cramQuery: Route = {
    path("query") {
      post {
        entity(as[TransferWgsCramV1QueryInput]) { input =>
          complete(wgsCramService.queryMetadata(input))
        }
      }
    }
  }

  private[webservice] val cramQueryall: Route = {
    path("queryall") {
      post {
        entity(as[TransferWgsCramV1QueryInput]) { input =>
          complete(wgsCramService.queryAllMetadata(input))
        }
      }
    }
  }

  private[webservice] val cramGetSchema: Route = {
    path("schema") {
      get {
        complete(wgsCramService.querySchema())
      }
    }
  }
}
