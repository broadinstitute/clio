package org.broadinstitute.clio.client.util

object ClassUtil {
  def formatFields(key: Object): String = {
    key.getClass.getDeclaredFields
      .map(field => {
        field.setAccessible(true)
        s"${field.getName}: ${field.get(key)}"
      })
      .mkString("[", ", ", "]")
  }
}
