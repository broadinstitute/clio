package org.broadinstitute.clio.server.dataaccess.elasticsearch

import java.net.URI
import java.time.OffsetDateTime
import java.util.UUID

import com.sksamuel.elastic4s.{HitReader, Indexable}
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.mappings.FieldDefinition
import enumeratum.EnumEntry
import org.broadinstitute.clio.util.generic.FieldMapper
import org.broadinstitute.clio.util.model.UpsertId

import scala.reflect.runtime.universe.Type

/**
  * Builds an ElasticsearchIndex using shapeless and reflection.
  *
  * @param name The base name of the index.
  * @param version The mapping version to use for the index.
  * @tparam Document The document being indexed.
  */
class AutoElasticsearchIndex[Document: FieldMapper: Indexable: HitReader] private[dataaccess] (
  name: String,
  private[elasticsearch] val version: Int
) extends ElasticsearchIndex[Document] {
  override val indexName: String =
    if (version == 1) {
      // We started out with no version suffix, so keep it that way for v1.
      name
    } else {
      s"${name}_v$version"
    }

  override val indexable: Indexable[Document] = implicitly[Indexable[Document]]

  override val hitReader: HitReader[Document] = implicitly[HitReader[Document]]

  override def fields: Seq[FieldDefinition] =
    AutoElasticsearchIndex.getFieldDefinitions[Document](version)
}

object AutoElasticsearchIndex {
  private def getFieldDefinitions[Document: FieldMapper](
    version: Int
  ): Seq[FieldDefinition] = {
    val stringToDef = stringToDefinition(version)

    FieldMapper[Document].fields.toSeq.sortBy {
      case (name, _) => name
    } map {
      case (name, value) =>
        stringToDef(value)(ElasticsearchUtil.toElasticsearchName(name))
    }
  }

  private def stringToDefinitionV1(
    fieldType: Type
  ): (String => FieldDefinition) = {
    import scala.reflect.runtime.universe.typeOf
    fieldType match {
      case tpe if tpe =:= typeOf[Boolean]           => booleanField
      case tpe if tpe =:= typeOf[Int]               => intField
      case tpe if tpe =:= typeOf[Long]              => longField
      case tpe if tpe =:= typeOf[Float]             => floatField
      case tpe if tpe =:= typeOf[Double]            => doubleField
      case tpe if tpe =:= typeOf[String]            => keywordField
      case tpe if tpe =:= typeOf[Symbol]            => keywordField
      case tpe if tpe <:< typeOf[EnumEntry]         => keywordField
      case tpe if tpe =:= typeOf[OffsetDateTime]    => dateField
      case tpe if tpe =:= typeOf[UpsertId]          => keywordField
      case tpe if tpe =:= typeOf[Option[Boolean]]   => booleanField
      case tpe if tpe =:= typeOf[Option[Int]]       => intField
      case tpe if tpe =:= typeOf[Option[Long]]      => longField
      case tpe if tpe =:= typeOf[Option[Float]]     => floatField
      case tpe if tpe =:= typeOf[Option[Double]]    => doubleField
      case tpe if tpe =:= typeOf[Option[String]]    => keywordField
      case tpe if tpe =:= typeOf[Option[Symbol]]    => keywordField
      case tpe if tpe <:< typeOf[Option[EnumEntry]] => keywordField
      case tpe if tpe =:= typeOf[Option[UUID]]      => keywordField
      case tpe if tpe =:= typeOf[Option[URI]]       => keywordField
      case tpe if tpe =:= typeOf[Option[OffsetDateTime]] =>
        dateField
      /*
       * Elasticsearch has no notion of an array field; instead, any
       * field can hold 1+ values of the same type.
       */
      case tpe if tpe <:< typeOf[Option[Seq[String]]] => keywordField
      case tpe if tpe <:< typeOf[Option[Seq[URI]]]    => keywordField
      case _ =>
        (fieldName: String) =>
          throw new IllegalArgumentException(
            s"No support for $fieldName: $fieldType"
          )
    }
  }

  private def stringToDefinitionV2(
    fieldType: Type
  ): (String => FieldDefinition) = {
    import scala.reflect.runtime.universe.typeOf

    /*
     * For Strings, create a top-level text field for searches,
     * with a nested keyword field for sorting / aggregations.
     */
    val textFieldWithKeyword =
      textField(_: String).fields(keywordField("exact"))

    fieldType match {
      case tpe if tpe =:= typeOf[String]              => textFieldWithKeyword
      case tpe if tpe =:= typeOf[Option[String]]      => textFieldWithKeyword
      case tpe if tpe <:< typeOf[Option[Seq[String]]] => textFieldWithKeyword
      case tpe                                        => stringToDefinitionV1(tpe)
    }
  }

  private def stringToDefinition(version: Int) = {
    version match {
      case 1 => stringToDefinitionV1 _
      case 2 => stringToDefinitionV2 _
      case _ => throw new IllegalArgumentException(s"No such version $version")
    }
  }
}
