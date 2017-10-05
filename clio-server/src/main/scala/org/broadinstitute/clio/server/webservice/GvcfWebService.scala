package org.broadinstitute.clio.server.webservice

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import org.broadinstitute.clio.server.service.GvcfService
import org.broadinstitute.clio.transfer.model.gvcf.{
  TransferGvcfV1Key,
  TransferGvcfV1Metadata,
  TransferGvcfV1QueryInput
}
import org.broadinstitute.clio.util.model.Location

trait GvcfWebService { self: JsonWebService =>

  def gvcfService: GvcfService

  lazy val gvcfRoutes: Route = {
    pathPrefix("v1") {
      pathPrefix("gvcf") {
        concat(gvcfGetSchema, gvcfPostMetadata, gvcfQuery, gvcfQueryall)
      }
    }
  }

  private[webservice] val gvcfPathPrefixKey: Directive1[TransferGvcfV1Key] = {
    for {
      location <- pathPrefix(Location.pathMatcher)
      project <- pathPrefix(Segment)
      sampleAlias <- pathPrefix(Segment)
      version <- pathPrefix(IntNumber)
    } yield TransferGvcfV1Key(location, project, sampleAlias, version)
  }

  private[webservice] val gvcfPostMetadata: Route = {
    pathPrefix("metadata") {
      gvcfPathPrefixKey { key =>
        post {
          entity(as[TransferGvcfV1Metadata]) { metadata =>
            complete(gvcfService.upsertMetadata(key, metadata))
          }
        }
      }
    }
  }

  private[webservice] val gvcfQuery: Route = {
    path("query") {
      post {
        entity(as[TransferGvcfV1QueryInput]) { input =>
          complete(gvcfService.queryMetadata(input))
        }
      }
    }
  }

  private[webservice] val gvcfQueryall: Route = {
    path("queryall") {
      post {
        entity(as[TransferGvcfV1QueryInput]) { input =>
          complete(gvcfService.queryAllMetadata(input))
        }
      }
    }
  }

  private[webservice] val gvcfGetSchema: Route = {
    path("schema") {
      get {
        complete(gvcfService.querySchema())
      }
    }
  }
}
