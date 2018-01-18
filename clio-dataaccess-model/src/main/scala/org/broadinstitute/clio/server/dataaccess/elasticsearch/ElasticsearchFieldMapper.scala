package org.broadinstitute.clio.server.dataaccess.elasticsearch

import java.net.URI
import java.time.OffsetDateTime
import java.util.UUID

import com.sksamuel.elastic4s.mappings.FieldDefinition
import enumeratum.EnumEntry
import enumeratum.values.{IntEnum, IntEnumEntry}
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex.TextExactMatchFieldName
import org.broadinstitute.clio.util.model.UpsertId

import scala.collection.immutable
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.Type

private[dataaccess] sealed abstract class ElasticsearchFieldMapper(
  override val value: Int
) extends IntEnumEntry {

  def stringToDefinition(fieldType: Type): (String => FieldDefinition)
}

private[dataaccess] object ElasticsearchFieldMapper
    extends IntEnum[ElasticsearchFieldMapper] {
  import com.sksamuel.elastic4s.http.ElasticDsl._

  override val values: immutable.IndexedSeq[ElasticsearchFieldMapper] = findValues

  /** All non-numeric, non-date members mapped to keywords. */
  case object InitVersion extends ElasticsearchFieldMapper(1) {

    override def stringToDefinition(
      fieldType: universe.Type
    ): String => FieldDefinition = {
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
  }

  /** String members to text fields, others (non-numeric, non-date) mapped to keywords. */
  case object StringsAsTextVersion extends ElasticsearchFieldMapper(2) {

    override def stringToDefinition(
      fieldType: universe.Type
    ): String => FieldDefinition = {
      import scala.reflect.runtime.universe.typeOf

      /*
       * For Strings, create a top-level text field for searches,
       * with a nested keyword field for sorting / aggregations.
       */
      val textFieldWithKeyword =
        textField(_: String).fields(keywordField(TextExactMatchFieldName))

      fieldType match {
        case tpe if tpe =:= typeOf[String]              => textFieldWithKeyword
        case tpe if tpe =:= typeOf[Option[String]]      => textFieldWithKeyword
        case tpe if tpe <:< typeOf[Option[Seq[String]]] => textFieldWithKeyword
        case tpe                                        => InitVersion.stringToDefinition(tpe)
      }
    }
  }
}
