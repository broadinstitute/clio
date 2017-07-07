package org.broadinstitute.clio.util.generic

import shapeless.labelled.FieldType
import shapeless.{::, HList, HNil, LabelledGeneric, Lazy, Witness}

import scala.reflect.runtime.universe.{Type, TypeTag, typeTag}

/**
  * Stores at compile time a map of fields with their scala reflection
  * [[http://docs.scala-lang.org/overviews/reflection/typetags-manifests.html Type]].
  *
  * Based very, very heavily on [[http://limansky.me/posts/2017-02-02-generating-sql-queries-with-shapeless.html]] and
  * [[http://underscore.io/books/shapeless-guide/]].
  *
  * @tparam A The type to retrieve fields.
  */
sealed trait FieldMapper[A] {
  def fields: Map[String, Type]
}

/**
  * Provides implicit FieldMapper instances.
  *
  * For example, to get fields for a case class Foo:
  * ```
  * case class Foo(a: Option[String], b: Int)
  *
  * val fieldMapper = FieldMapper[Foo]
  * println(fieldMapper.fields)
  * ```
  *
  * @see [[org.broadinstitute.clio.util.json.StateDecoder]]
  * @see [[org.broadinstitute.clio.util.generic.CaseClassMapper]]
  * @see [[org.broadinstitute.clio.util.generic.CaseClassMapperWithTypes]]
  */
object FieldMapper {

  /**
    * Summon any implicit mapper that implements FieldMapper[A].
    *
    * https://github.com/underscoreio/shapeless-guide/blob/0.3/src/pages/generic/type-classes.md#idiomatic-type-class-definitions-secgenericidiomatic-style
    */
  def apply[A](implicit mapper: FieldMapper[A]): FieldMapper[A] = mapper

  /**
    * Utility to create a mapper based on constant map of fields.
    *
    * @param mapperFields The fields for A.
    * @tparam A The type to retrieve fields.
    * @return Mapper instance with the passed fields.
    */
  private def createMapper[A](
    mapperFields: Map[String, Type]
  ): FieldMapper[A] = {
    new FieldMapper[A] {
      override val fields: Map[String, Type] = mapperFields
    }
  }

  /**
    * A mapper for HNil, returning no fields.
    */
  implicit val hnilMapper: FieldMapper[HNil] = createMapper(Map.empty)

  /**
    * Returns a FieldMapper for any HList made up of a (headNameAndOptionType :: (more tail elements) :: HNil).
    *
    * The headNameAndType is actually made up of a FieldType, which is a tuple of [[scala.Symbol]] plus some type H.
    *
    * https://github.com/underscoreio/shapeless-guide/blob/0.3/src/pages/labelled-generic/products.md#deriving-product-instances-with-labelledgeneric
    * https://youtu.be/Zt6LjUnOcFQ?t=39m30s
    *
    * @param witness    Reads the symbol name.
    * @param tailMapper An implicit mapper for the tail. Either this method recursively, minus the first element, or
    *                   just the hnilMapper.
    * @tparam K The singleton-type of the head. When used with the witness allows access to the field name.
    * @tparam H The type of the head, with a context bound also specifying that an `implicit ev: Decoder[H]` exists.
    *           https://www.scala-lang.org/files/archive/spec/2.12/07-implicits.html#context-bounds-and-view-bounds
    * @tparam T The tail of the list, some list of headNameAndType :: tail.
    * @return A mapper of the fields in the head-plus-tail.
    */
  implicit def hlistMapper[K <: Symbol, H: TypeTag, T <: HList](
    implicit
    witness: Witness.Aux[K],
    tailMapper: FieldMapper[T]
  ): FieldMapper[FieldType[K, H] :: T] = {
    createMapper(
      Map(witness.value.name -> typeTag[H].tpe) ++ tailMapper.fields
    )
  }

  /**
    * Creates a decoder for any type that can be converted to an HList representation. The HList must be a labeled
    * version so that the names may be retrieved.
    *
    * https://github.com/underscoreio/shapeless-guide/blob/0.3/src/pages/representations/products.md#generic-product-encodings
    * https://github.com/underscoreio/shapeless-guide/blob/0.3/src/pages/labelled-generic/literal-types.md#type-tagging-and-phantom-types-seclabelled-generictype-tagging
    *
    * @param gen    Implicit evidence that A has a generic representation ARepr.
    * @param mapper A mapper for the generic representation ARepr, created by implicitly calling hlistMapper.
    * @tparam A     The original type.
    * @tparam ARepr The shapeless representation of A.
    * @return A mapper for the fields in A.
    */
  implicit def genericMapper[A, ARepr <: HList](
    implicit
    gen: LabelledGeneric.Aux[A, ARepr],
    mapper: Lazy[FieldMapper[ARepr]]
  ): FieldMapper[A] = {
    require(
      gen != null,
      "gen is only used as evidence that A has the generic representation ARepr. " +
        "Without this 'require' the compiler complains that 'gen' is unused."
    )
    createMapper(mapper.value.fields)
  }
}
