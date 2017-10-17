package org.broadinstitute.clio.server.dataaccess.elasticsearch

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
}
