package org.broadinstitute.clio.server.dataaccess.elasticsearch

import java.time.OffsetDateTime
import java.util.UUID

import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.mappings.FieldDefinition
import enumeratum.EnumEntry
import org.broadinstitute.clio.util.generic.FieldMapper
import org.broadinstitute.clio.util.model.UpsertId

import scala.reflect.runtime.universe.Type

/**
  * Builds an ElasticsearchIndex using shapeless and reflection.
  *
  * @param indexName The name of the index.
  * @tparam Document The document being indexed.
  */
class AutoElasticsearchIndex[Document: FieldMapper] private[dataaccess] (
  override val indexName: String
) extends ElasticsearchIndex[Document] {
  override def fields: Seq[FieldDefinition] =
    AutoElasticsearchIndex.getFieldDefinitions[Document]
}

object AutoElasticsearchIndex {
  private def getFieldDefinitions[Document: FieldMapper]
    : Seq[FieldDefinition] = {
    FieldMapper[Document].fields.toSeq.sortBy {
      case (name, _) => name
    } map {
      case (name, value) => getFieldDefinition(name, value)
    }
  }

  private def getFieldDefinition(fieldName: String,
                                 fieldType: Type): FieldDefinition = {
    import scala.reflect.runtime.universe.typeOf
    val stringToDefinition = fieldType match {
      case tpe if tpe =:= typeOf[Boolean]           => booleanField _
      case tpe if tpe =:= typeOf[Int]               => intField _
      case tpe if tpe =:= typeOf[Long]              => longField _
      case tpe if tpe =:= typeOf[Float]             => floatField _
      case tpe if tpe =:= typeOf[Double]            => doubleField _
      case tpe if tpe =:= typeOf[String]            => keywordField _
      case tpe if tpe <:< typeOf[EnumEntry]         => keywordField _
      case tpe if tpe =:= typeOf[OffsetDateTime]    => dateField _
      case tpe if tpe =:= typeOf[UUID]              => keywordField _
      case tpe if tpe =:= typeOf[UpsertId]          => keywordField _
      case tpe if tpe =:= typeOf[Option[Boolean]]   => booleanField _
      case tpe if tpe =:= typeOf[Option[Int]]       => intField _
      case tpe if tpe =:= typeOf[Option[Long]]      => longField _
      case tpe if tpe =:= typeOf[Option[Float]]     => floatField _
      case tpe if tpe =:= typeOf[Option[Double]]    => doubleField _
      case tpe if tpe =:= typeOf[Option[String]]    => keywordField _
      case tpe if tpe <:< typeOf[Option[EnumEntry]] => keywordField _
      case tpe if tpe =:= typeOf[Option[OffsetDateTime]] =>
        dateField _
      case _ =>
        throw new IllegalArgumentException(
          s"No support for $fieldName: $fieldType"
        )
    }
    stringToDefinition(ElasticsearchUtil.toElasticsearchName(fieldName))
  }
}
