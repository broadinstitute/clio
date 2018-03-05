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
import scala.reflect.runtime.universe.{Type, TypeTag, typeOf}

private[dataaccess] sealed abstract class ElasticsearchFieldMapper(
  override val value: Int
) extends IntEnumEntry {

  def stringToDefinition(fieldType: Type): String => FieldDefinition

  def valueToQuery(fieldName: String, fieldType: Type): Any => QueryDefinition

  protected def is[A: TypeTag](tpe: Type): Boolean = {
    /*
     * Elasticsearch has no notion of required or array fields; instead, any
     * field can hold 0+ values of the same type.
     */
    Seq(typeOf[A], typeOf[Option[A]], typeOf[Seq[A]], typeOf[Option[Seq[A]]])
      .exists(tpe <:< _)
  }
}

object ElasticsearchFieldMapper
    extends IntEnum[ElasticsearchFieldMapper] {
  import com.sksamuel.elastic4s.http.ElasticDsl._

  override val values: immutable.IndexedSeq[ElasticsearchFieldMapper] = findValues

  case object NumericBooleanDateAndKeywordFields extends ElasticsearchFieldMapper(1) {

    override def stringToDefinition(fieldType: Type): String => FieldDefinition = {
      fieldType match {
        case tpe if is[Boolean](tpe)        => booleanField
        case tpe if is[Int](tpe)            => intField
        case tpe if is[Long](tpe)           => longField
        case tpe if is[Float](tpe)          => floatField
        case tpe if is[Double](tpe)         => doubleField
        case tpe if is[String](tpe)         => keywordField
        case tpe if is[Symbol](tpe)         => keywordField
        case tpe if is[EnumEntry](tpe)      => keywordField
        case tpe if is[UpsertId](tpe)       => keywordField
        case tpe if is[UUID](tpe)           => keywordField
        case tpe if is[URI](tpe)            => keywordField
        case tpe if is[OffsetDateTime](tpe) => dateField
        case _ =>
          (fieldName: String) =>
            throw new IllegalArgumentException(
              s"No support for $fieldName: $fieldType"
            )
      }
    }

    override def valueToQuery(
      fieldName: String,
      fieldType: Type
    ): Any => QueryDefinition = {
      fieldType match {
        case tpe if is[OffsetDateTime](tpe) && fieldName.endsWith("_start") =>
          value =>
            rangeQuery(fieldName.stripSuffix("_start"))
              .gte(value.asInstanceOf[OffsetDateTime].toString)
        case tpe if is[OffsetDateTime](tpe) && fieldName.endsWith("_end") =>
          value =>
            rangeQuery(fieldName.stripSuffix("_end"))
              .lte(value.asInstanceOf[OffsetDateTime].toString)
        case tpe if is[Symbol](tpe) =>
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
        case tpe if is[String](tpe) => textFieldWithKeyword
        case tpe                    => NumericBooleanDateAndKeywordFields.stringToDefinition(tpe)
      }
    }

    override def valueToQuery(
      fieldName: String,
      fieldType: Type
    ): Any => QueryDefinition = {
      fieldType match {
        case tpe if is[String](tpe) =>
          NumericBooleanDateAndKeywordFields.valueToQuery(
            s"$fieldName.$TextExactMatchFieldName",
            tpe
          )
        case tpe => NumericBooleanDateAndKeywordFields.valueToQuery(fieldName, tpe)
      }
    }
  }
}
