package org.broadinstitute.clio.server.dataaccess.elasticsearch

import java.io.IOException

import com.sksamuel.elastic4s.http.{ElasticClient, Handler, RequestFailure}
import org.broadinstitute.clio.util.generic.CirceEquivalentCamelCaseLexer
import s_mach.string._

import scala.concurrent.{ExecutionContext, Future}

object ElasticsearchUtil {

  /**
    * Convert a scala name (such as type or field) to its corresponding elastic
    * search name.
    *
    * @param scalaName the name to convert
    * @return the name as an elastic search name
    */
  def toElasticsearchName(scalaName: String): String =
    scalaName.toSnakeCase(CirceEquivalentCamelCaseLexer)

  class RequestException(val requestFailure: RequestFailure)
      extends IOException(s"Error in Elasticsearch request: ${requestFailure.error}")

  /**
    * Wrapper for elastic4's [[ElasticClient]], used to add common patterns as extension methods.
    */
  implicit class HttpClientOps(val elasticClient: ElasticClient) extends AnyVal {

    /**
      * Send an HTTP request to Elasticsearch, and fold away the nested `Either`
      * provided by elastic4s in the response.
      */
    def executeAndUnpack[T, U](request: T)(
      implicit handler: Handler[T, U],
      manifest: Manifest[U],
      executionContext: ExecutionContext
    ): Future[U] = {
      elasticClient
        .execute(request)
        .map(
          response ⇒
            if (response.isError)
              throw new RequestException(
                new RequestFailure(
                  response.status,
                  response.body,
                  response.headers,
                  response.error
                )
              )
            else response.result
        )
    }

  }
}
