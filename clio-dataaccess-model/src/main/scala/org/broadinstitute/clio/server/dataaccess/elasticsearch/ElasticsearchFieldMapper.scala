package org.broadinstitute.clio.server.dataaccess.elasticsearch

import java.net.URI
import java.time.OffsetDateTime
import java.util.UUID

import com.sksamuel.elastic4s.mappings.FieldDefinition
import com.sksamuel.elastic4s.searches.queries.QueryDefinition
import enumeratum.EnumEntry
import enumeratum.values.{IntEnum, IntEnumEntry}
import org.broadinstitute.clio.util.model.UpsertId

import scala.collection.immutable
import scala.reflect.runtime.universe.{Type, typeOf}

private[dataaccess] sealed abstract class ElasticsearchFieldMapper(
  override val value: Int
) extends IntEnumEntry {

  def stringToDefinition(fieldType: Type): String => FieldDefinition

  def valueToQuery(fieldName: String, fieldType: Type): Any => QueryDefinition
}

private[dataaccess] object ElasticsearchFieldMapper
    extends IntEnum[ElasticsearchFieldMapper] {
  import com.sksamuel.elastic4s.http.ElasticDsl._

  override val values: immutable.IndexedSeq[ElasticsearchFieldMapper] = findValues

  case object NumericBooleanDateAndKeywordFields extends ElasticsearchFieldMapper(1) {

    override def stringToDefinition(fieldType: Type): String => FieldDefinition = {
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

    private def isDate(tpe: Type): Boolean =
      tpe =:= typeOf[OffsetDateTime] || tpe =:= typeOf[Option[OffsetDateTime]]

    private def isSymbol(tpe: Type): Boolean =
      tpe =:= typeOf[Symbol] || tpe =:= typeOf[Option[Symbol]]

    override def valueToQuery(
      fieldName: String,
      fieldType: Type
    ): Any => QueryDefinition = {
      fieldType match {
        case tpe if isDate(tpe) && fieldName.endsWith("_start") =>
          value =>
            rangeQuery(fieldName.stripSuffix("_start"))
              .gte(value.asInstanceOf[OffsetDateTime].toString)
        case tpe if isDate(tpe) && fieldName.endsWith("_end") =>
          value =>
            rangeQuery(fieldName.stripSuffix("_end"))
              .lte(value.asInstanceOf[OffsetDateTime].toString)
        case tpe if isSymbol(tpe) =>
          value =>
            queryStringQuery(s""""${value.asInstanceOf[Symbol].name}"""")
              .defaultField(fieldName)
        case _ =>
          value =>
            queryStringQuery(s""""$value"""").defaultField(fieldName)
      }
    }
  }

  case object StringsToTextFieldsWithSubKeywords extends ElasticsearchFieldMapper(2) {

    /**
      * Name to assign to a nested keyword field under every text field,
      * to support exact matching.
      */
    private[elasticsearch] val TextExactMatchFieldName: String = "exact"

    override def stringToDefinition(fieldType: Type): String => FieldDefinition = {
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
        case tpe                                        => NumericBooleanDateAndKeywordFields.stringToDefinition(tpe)
      }
    }

    private def isString(tpe: Type): Boolean =
      tpe =:= typeOf[String] || tpe =:= typeOf[Option[String]] || tpe <:< typeOf[Option[
        Seq[String]
      ]]

    override def valueToQuery(
      fieldName: String,
      fieldType: Type
    ): Any => QueryDefinition = {
      fieldType match {
        case tpe if isString(tpe) =>
          NumericBooleanDateAndKeywordFields.valueToQuery(
            s"$fieldName.$TextExactMatchFieldName",
            tpe
          )
        case tpe => NumericBooleanDateAndKeywordFields.valueToQuery(fieldName, tpe)
      }
    }
  }
}
