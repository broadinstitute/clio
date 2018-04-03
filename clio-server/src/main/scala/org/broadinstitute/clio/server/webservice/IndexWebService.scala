package org.broadinstitute.clio.server.webservice

import akka.http.scaladsl.server._
import akka.http.scaladsl.server.Directives._
import akka.stream.Materializer
import org.broadinstitute.clio.server.service.IndexService
import org.broadinstitute.clio.transfer.model._
import org.broadinstitute.clio.util.ApiConstants._

abstract class IndexWebService[CI <: ClioIndex](
  val indexService: IndexService[CI]
)(implicit materializer: Materializer)
    extends JsonWebService {

  import indexService.clioIndex.implicits._

  lazy val routes: Route = {
    pathPrefix("v1") {
      pathPrefix(indexService.clioIndex.urlSegment) {
        concat(postMetadata, query, queryall)
      }
    }
  }

  private[webservice] def pathPrefixKey: Directive1[indexService.clioIndex.KeyType]

  private[webservice] val postMetadata: Route = {
    pathPrefix(metadataString) {
      parameter(Symbol(forceString).as[Boolean] ? false) { force =>
        pathPrefixKey { key =>
          post {
            entity(as[indexService.clioIndex.MetadataType]) { metadata =>
              complete(indexService.upsertMetadata(key, metadata, force))
            }
          }
        }
      }
    }
  }

  private[webservice] val query: Route = {
    path(queryString) {
      post {
        entity(as[indexService.clioIndex.QueryInputType]) { input =>
          complete(indexService.queryMetadata(input))
        }
      }
    }
  }

  private[webservice] val queryall: Route = {
    path(queryAllString) {
      post {
        entity(as[indexService.clioIndex.QueryInputType]) { input =>
          complete(indexService.queryAllMetadata(input))
        }
      }
    }
  }
}
