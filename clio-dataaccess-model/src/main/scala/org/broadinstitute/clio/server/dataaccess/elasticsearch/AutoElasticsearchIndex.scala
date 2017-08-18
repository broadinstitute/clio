package org.broadinstitute.clio.server.dataaccess.elasticsearch

import org.broadinstitute.clio.util.generic.FieldMapper

import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.mappings.FieldDefinition
import enumeratum.EnumEntry

import scala.reflect.runtime.universe.Type

import java.time.OffsetDateTime

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
    import s_mach.string._
    val nameSnake = fieldName.toSnakeCase(Lexer.CamelCase)
    import scala.reflect.runtime.universe.typeOf
    fieldType match {
      case tpe if tpe =:= typeOf[Boolean]           => booleanField(nameSnake)
      case tpe if tpe =:= typeOf[Int]               => intField(nameSnake)
      case tpe if tpe =:= typeOf[Long]              => longField(nameSnake)
      case tpe if tpe =:= typeOf[Float]             => floatField(nameSnake)
      case tpe if tpe =:= typeOf[Double]            => doubleField(nameSnake)
      case tpe if tpe =:= typeOf[String]            => keywordField(nameSnake)
      case tpe if tpe <:< typeOf[EnumEntry]         => keywordField(nameSnake)
      case tpe if tpe =:= typeOf[OffsetDateTime]    => dateField(nameSnake)
      case tpe if tpe =:= typeOf[Option[Boolean]]   => booleanField(nameSnake)
      case tpe if tpe =:= typeOf[Option[Int]]       => intField(nameSnake)
      case tpe if tpe =:= typeOf[Option[Long]]      => longField(nameSnake)
      case tpe if tpe =:= typeOf[Option[Float]]     => floatField(nameSnake)
      case tpe if tpe =:= typeOf[Option[Double]]    => doubleField(nameSnake)
      case tpe if tpe =:= typeOf[Option[String]]    => keywordField(nameSnake)
      case tpe if tpe <:< typeOf[Option[EnumEntry]] => keywordField(nameSnake)
      case tpe if tpe =:= typeOf[Option[OffsetDateTime]] =>
        dateField(nameSnake)
      case _ =>
        throw new IllegalArgumentException(
          s"No support for $fieldName: $fieldType"
        )
    }
  }
}
