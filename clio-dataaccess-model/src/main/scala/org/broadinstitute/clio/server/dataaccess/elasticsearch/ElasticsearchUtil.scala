package org.broadinstitute.clio.server.dataaccess.elasticsearch

import java.io.IOException

import com.sksamuel.elastic4s.http.{HttpClient, HttpExecutable, RequestFailure}
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
    * Wrapper for elastic4's [[HttpClient]], used to add common patterns as extension methods.
    */
  implicit class HttpClientOps(val httpClient: HttpClient) extends AnyVal {

    /**
      * Send an HTTP request to Elasticsearch, and fold away the nested `Either`
      * provided by elastic4s in the response.
      */
    def executeAndUnpack[T, U](request: T)(
      implicit exec: HttpExecutable[T, U],
      executionContext: ExecutionContext
    ): Future[U] = {
      httpClient
        .execute(request)
        .map(_.fold(failure => throw new RequestException(failure), _.result))
    }

  }
}
