package org.broadinstitute.clio.server.service

import java.time.OffsetDateTime

import io.circe._
import org.broadinstitute.clio.transfer.model.TransferReadGroupV1QueryOutput
import shapeless._
import shapeless.labelled._

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.runtime.universe.{Type, TypeTag, typeOf, typeTag}

/**
  * Generate JSON schema from case classes such as the QueryOutputs.
  *
  * JsonSchema uses the same shapeless pattern as the
  * [[org.broadinstitute.clio.util.generic.FieldMapper]]
  */
object SchemaService {

  /**
    * a JSON schema for A suitable for Circe
    *
    * @tparam A is a case class such as a QueryOutput
    */
  trait JsonSchema[A] {

    /**
      * @return the names of fields required in A
      */
    def required: Set[String]

    /**
      * @return the JSON properties of all fields in A
      */
    def properties: Map[String, Json]

    /**
      * @return a Circe representation of the schema
      */
    def toJson: Json
  }

  object JsonSchema {

    /**
      * Summon a JsonSchema encoder from implicit scope.
      *
      * @param jsonSchema to summon
      * @tparam A is a case class such as a QueryOutput
      * @return the implicit JsonSchema object
      */
    def apply[A](implicit jsonSchema: JsonSchema[A]): JsonSchema[A] =
      jsonSchema

    /**
      * Specialize the JsonSchema trait to combine theRequired and
      * theProperties into a representation of a JsonSchema for A.
      *
      * @param theRequired fields are typed without Option
      * @param theProperties of the fields describe their types
      * @tparam A is a case class such as a QueryOutput
      * @return a new JSON schema representation for A
      */
    def makeSchema[A](theRequired: Set[String],
                      theProperties: Map[String, Json]): JsonSchema[A] =
      new JsonSchema[A] {
        override val required: Set[String] = theRequired
        override val properties: Map[String, Json] = theProperties
        private val typeObject = "type" -> Json.fromString("object")
        private val allRequired = "required" -> Json.fromValues(
          required map Json.fromString
        )
        private val allProperties = "properties" -> Json.fromFields(properties)
        override val toJson: Json =
          Json.obj(typeObject, allRequired, allProperties)
      }

    /**
      * The JSON schema for an empty class is empty.
      */
    implicit val hnilSchema: JsonSchema[HNil] =
      makeSchema(Set.empty, Map.empty)

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
    def makeProperty(fieldName: String, fieldType: Type): (Boolean, Json) = {
      val aBoolean = Json.obj(aType("boolean"))
      val aString = Json.obj(aType("string"))
      val aInt = Json.obj(aType("integer"), aFormat("int32"))
      val aLong = Json.obj(aType("integer"), aFormat("int64"))
      val aTime = Json.obj(aType("string"), aFormat("date-time"))
      fieldType match {
        case t if t =:= typeOf[Boolean]                => (true, aBoolean)
        case t if t =:= typeOf[Int]                    => (true, aInt)
        case t if t =:= typeOf[Long]                   => (true, aLong)
        case t if t =:= typeOf[String]                 => (true, aString)
        case t if t =:= typeOf[OffsetDateTime]         => (true, aTime)
        case t if t =:= typeOf[Option[Boolean]]        => (false, aBoolean)
        case t if t =:= typeOf[Option[Int]]            => (false, aInt)
        case t if t =:= typeOf[Option[Long]]           => (false, aLong)
        case t if t =:= typeOf[Option[String]]         => (false, aString)
        case t if t =:= typeOf[Option[OffsetDateTime]] => (false, aTime)
        case _ =>
          throw new IllegalArgumentException(
            s"No JsonSchema support for $fieldName: $fieldType yet"
          )
      }
    }

    /**
      * a JSON schema for a shapeless HList representing a class
      *
      * @param witness is a witness for the field name in the head
      * @param tail is the tail of the
      * @tparam Key is a singleton type for the name of the head field
      * @tparam Head is the type of the head field
      * @tparam Tail is the type of the rest of the fields after head
      * @return a JSON schema for the HList representation of A
      */
    implicit def hlistSchema[Key <: Symbol, Head: TypeTag, Tail <: HList](
      implicit witness: Witness.Aux[Key],
      tail: JsonSchema[Tail]
    ): JsonSchema[FieldType[Key, Head] :: Tail] = {
      import s_mach.string._
      val fieldName = witness.value.name.toSnakeCase(Lexer.CamelCase)
      val property = makeProperty(fieldName, typeTag[Head].tpe)
      val head = property match {
        case (true, json)  => (Set(fieldName), Map(fieldName -> json))
        case (false, json) => (Set.empty, Map(fieldName -> json))
      }
      head match {
        case (req, prop) =>
          makeSchema(req ++ tail.required, prop ++ tail.properties)
      }
    }

    /**
      * a JSON schema for an A that has an HList representation
      *
      * @param gen to bind A to its representation preserving names
      * @param schema is the lazy representation of the result
      * @tparam A is the class to generate a schema for
      * @tparam ARepr is the HList representation of A
      * @return a JsonSchema for A
      */
    implicit def genericSchema[A, ARepr <: HList](
      implicit gen: LabelledGeneric.Aux[A, ARepr],
      schema: Lazy[JsonSchema[ARepr]]
    ): JsonSchema[A] = {
      require(
        gen != null,
        "Without this the compiler complains that 'gen' is unused."
      )
      makeSchema(schema.value.required, schema.value.properties)
    }
  }

  /**
    * The JSON schema for a ReadGroup index
    */
  lazy val readGroupSchemaJson: Json =
    JsonSchema[TransferReadGroupV1QueryOutput].toJson

  /**
    * a Future JSON schema for a ReadGroup index
    *
    * @param executionContext is from the webservice
    * @return a marshallable JSON schema for a ReadGroup
    */
  def readGroupSchema(
    implicit executionContext: ExecutionContext
  ): Future[Json] = Future(readGroupSchemaJson)
}
