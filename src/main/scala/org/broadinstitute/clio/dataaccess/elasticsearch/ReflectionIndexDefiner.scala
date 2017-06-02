package org.broadinstitute.clio.dataaccess.elasticsearch

import java.lang.reflect.Field

import org.broadinstitute.clio.model.{ElasticsearchField, ElasticsearchIndex}

class ReflectionIndexDefiner[A](indexName: String, indexType: String, indexClass: Class[A])
  extends ElasticsearchIndexDefiner[A] {
  override def indexDefinition: ElasticsearchIndex = ElasticsearchIndex(indexName, indexType, fieldDefinitions)

  lazy val fieldDefinitions = indexClass.getDeclaredFields map ReflectionIndexDefiner.getFieldDefinition
}

object ReflectionIndexDefiner {
  private def getFieldDefinition(field: Field) = ElasticsearchField(camelToSnake(field.getName), field.getType)

  private val CamelRegex = "[A-Z]".r

  private def camelToSnake(value: String): String = {
    CamelRegex.replaceAllIn(value, matcher => s"_${matcher.group(0).toLowerCase}")
  }
}
