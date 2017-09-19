package org.broadinstitute.clio.server.dataaccess.elasticsearch

import s_mach.string._

object ElasticSearchUtil {

  /**
    * Convert a scala name (such as type or field) to its corresponding elastic
    * search name.
    *
    * @param scalaName the name to convert
    * @return the name as an elastic search name
    */
  def toElasticSearchName(scalaName: String): String =
    scalaName.toSnakeCase(Lexer.CamelCase)
}
