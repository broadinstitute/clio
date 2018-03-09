package org.broadinstitute.clio.server.webservice

import akka.http.scaladsl.server._
import akka.http.scaladsl.server.Directives._
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ClioDocument,
  ElasticsearchIndex
}
import org.broadinstitute.clio.server.service.IndexService
import org.broadinstitute.clio.transfer.model.TransferIndex

abstract class IndexWebService[TI <: TransferIndex, D <: ClioDocument: ElasticsearchIndex](
  val indexService: IndexService[TI, D]
) extends JsonWebService {

  import indexService.transferIndex.implicits._

  lazy val routes: Route = {
    pathPrefix("v1") {
      pathPrefix(indexService.transferIndex.urlSegment) {
        concat(getSchema, postMetadata, query, queryall)
      }
    }
  }

  private[webservice] def pathPrefixKey: Directive1[indexService.transferIndex.KeyType]

  private[webservice] val postMetadata: Route = {
    pathPrefix("metadata") {
      pathPrefixKey { key =>
        post {
          entity(as[indexService.transferIndex.MetadataType]) { metadata =>
            complete(indexService.upsertMetadata(key, metadata))
          }
        }
      }
    }
  }

  private[webservice] val query: Route = {
    path("query") {
      post {
        entity(as[indexService.transferIndex.QueryInputType]) { input =>
          complete(indexService.queryMetadata(input))
        }
      }
    }
  }

  private[webservice] val queryall: Route = {
    path("queryall") {
      post {
        entity(as[indexService.transferIndex.QueryInputType]) { input =>
          complete(indexService.queryAllMetadata(input))
        }
      }
    }
  }

  private[webservice] val getSchema: Route = {
    path("schema") {
      get {
        complete(indexService.querySchema())
      }
    }
  }
}
