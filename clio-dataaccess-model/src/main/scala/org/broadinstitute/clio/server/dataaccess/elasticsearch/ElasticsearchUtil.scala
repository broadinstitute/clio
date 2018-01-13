package org.broadinstitute.clio.server.dataaccess.elasticsearch

import java.io.IOException

import com.sksamuel.elastic4s.http.{RequestFailure, RequestSuccess}
import org.broadinstitute.clio.util.generic.CirceEquivalentCamelCaseLexer
import s_mach.string._

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

  def unpackResponse[T](response: Either[RequestFailure, RequestSuccess[T]]): T = {
    response.fold(failure => throw new RequestException(failure), _.result)
  }
}
