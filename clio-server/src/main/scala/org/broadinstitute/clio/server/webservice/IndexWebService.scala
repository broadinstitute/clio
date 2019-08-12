package org.broadinstitute.clio.server.webservice

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.stream.scaladsl.{Sink, Source}
import com.typesafe.scalalogging.StrictLogging
import io.circe.JsonObject
import org.broadinstitute.clio.server.service.IndexService
import org.broadinstitute.clio.transfer.model.ApiConstants._
import org.broadinstitute.clio.transfer.model._
import org.broadinstitute.clio.util.model.UpsertId

object IndexWebService {

  def decodeSlashes(s: String): String =
    s.replace("%2F", "/")
}

abstract class IndexWebService[CI <: ClioIndex](
  val indexService: IndexService[CI]
) extends JsonWebService
    with StrictLogging {

  import indexService.clioIndex.implicits._

  lazy val routes: Route = {
    pathPrefix("v1") {
      pathPrefix(indexService.clioIndex.urlSegment) {
        concat(postMetadata, query, rawquery)
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
              extractRequestContext { ctx =>
                import ctx.materializer

                complete {
                  // Type inference failed for me just trying to return this,
                  // but maybe we can massage it in a different way
                  val fut = indexService
                    .upsertMetadata(key, metadata, force)
                    //we force run this stream here to get a strict Future to avoid creating a chunked HttpEntity
                    .runWith(Sink.headOption[UpsertId])
                  fut
                }
              }
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

  private[webservice] val rawquery: Route = {
    path(rawQueryString) {
      post {
        entity(as[JsonObject]) { json =>
          extractRequestContext { ctx =>
            import ctx.materializer
            import ctx.executionContext

            // we "prime" the response by pulling its head because
            // otherwise, an initial successful response is sent to the client
            // along with a stream to pull on no matter what. If we cannot then get a successful
            // response from elasticsearch, the stream is truncated and an unhelpful exception
            // is thrown in the client. This way, we at least get to indicate that a problem occurred in the server
            // with a 500 response.
            val primedResponse =
              indexService
                .rawQuery(json)
                .prefixAndTail(1)
                .runWith(Sink.head)
            complete {
              primedResponse.map {
                case (seq, source) => Source(seq).concat(source)
              }
            }
          }
        }
      }
    }
  }

}
