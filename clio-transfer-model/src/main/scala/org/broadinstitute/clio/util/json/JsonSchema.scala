package org.broadinstitute.clio.util.json

import java.net.URI

import io.circe.Json

import scala.reflect.runtime.universe.{Type, typeOf}
import java.time.OffsetDateTime
import java.util.UUID

import enumeratum.EnumEntry
import org.broadinstitute.clio.transfer.model.ClioIndex
import org.broadinstitute.clio.util.generic.{CirceEquivalentCamelCaseLexer, FieldMapper}

/**
  * A JSON schema for a key / metadata pair in Clio.
  *
  * @tparam CI The ClioIndex of the key / metadata pair.
  */
class JsonSchema[CI <: ClioIndex](index: CI) {
  import index.implicits._
  import s_mach.string._

  /**
    * @return the names of fields required in A
    */
  lazy val required: Set[String] = FieldMapper[index.KeyType].fields.keySet
    .map(_.toSnakeCase(CirceEquivalentCamelCaseLexer))

  /**
    * @return the JSON properties of all fields in A
    */
  lazy val properties: Map[String, Json] = Iterable
    .concat(
      FieldMapper[index.KeyType].fields,
      FieldMapper[index.MetadataType].fields
    )
    .map {
      case (name, tpe) =>
        val snakeName = name.toSnakeCase(CirceEquivalentCamelCaseLexer)
        snakeName -> makeProperty(snakeName, tpe)
    }
    .toMap

  /**
    * @return a Circe representation of the schema
    */
  lazy val toJson: Json = Json.obj(
    "type" -> Json.fromString("object"),
    "required" -> Json.fromValues(required.map(Json.fromString)),
    "properties" -> Json.fromFields(properties)
  )

  /**
    * type and format pairs for building JSON property schema
    *
    * @param s is the name of a JSON schema type
    * @return a type or format pair for s
    */
  private def aType(s: String) = "type" -> Json.fromString(s)
  private def aFormat(s: String) = "format" -> Json.fromString(s)

  /**
    * a required property pair for field named fieldName of fieldType
    *
    * @param fieldName is the name of the field
    * @param fieldType is the type of the field
    * @return true if the field is required and its type property
    */
  private def makeProperty(fieldName: String, fieldType: Type): Json = {
    val aBoolean = Json.obj(aType("boolean"))
    val aString = Json.obj(aType("string"))
    val aStringArray = Json.obj(aType("string"), aFormat("array"))
    val aInt = Json.obj(aType("integer"), aFormat("int32"))
    val aLong = Json.obj(aType("integer"), aFormat("int64"))
    val aTime = Json.obj(aType("string"), aFormat("date-time"))
    val aFloat = Json.obj(aType("float"), aFormat("float32"))
    val aDouble = Json.obj(aType("float"), aFormat("float64"))
    val aUri = Json.obj(aType("string"), aFormat("uri"))
    val aUriArray = Json.obj(aType("string"), aFormat("uri-array"))
    fieldType match {
      case t if t =:= typeOf[Boolean]                => aBoolean
      case t if t =:= typeOf[Int]                    => aInt
      case t if t =:= typeOf[Long]                   => aLong
      case t if t =:= typeOf[String]                 => aString
      case t if t =:= typeOf[Float]                  => aFloat
      case t if t =:= typeOf[Double]                 => aDouble
      case t if t <:< typeOf[EnumEntry]              => aString
      case t if t =:= typeOf[OffsetDateTime]         => aTime
      case t if t =:= typeOf[Symbol]                 => aString
      case t if t =:= typeOf[Option[Boolean]]        => aBoolean
      case t if t =:= typeOf[Option[Int]]            => aInt
      case t if t =:= typeOf[Option[Long]]           => aLong
      case t if t =:= typeOf[Option[String]]         => aString
      case t if t =:= typeOf[Option[Float]]          => aFloat
      case t if t =:= typeOf[Option[Double]]         => aDouble
      case t if t <:< typeOf[Option[EnumEntry]]      => aString
      case t if t =:= typeOf[Option[OffsetDateTime]] => aTime
      case t if t =:= typeOf[Option[Symbol]]         => aString
      case t if t =:= typeOf[Option[UUID]]           => aString
      case t if t =:= typeOf[Option[URI]]            => aUri
      case t if t <:< typeOf[Option[Seq[String]]]    => aStringArray
      case t if t <:< typeOf[Option[Seq[URI]]]       => aUriArray
      case _ =>
        throw new IllegalArgumentException(
          s"No JsonSchema support for $fieldName: $fieldType yet"
        )
    }
  }
}
