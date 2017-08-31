package org.broadinstitute.clio.util.generic

import shapeless.{HList, LabelledGeneric}

import scala.annotation.implicitNotFound

/**
  * Facilitates converting between two case class types that have the same members.
  *
  * A trimmed down version of
  * [[https://stackoverflow.com/questions/29242873/shapeless-turn-a-case-class-into-another-with-fields-in-different-order#29243997]]
  *
  * Doesn't use `shapeless.ops.hlist._` as the compilation times for `Prepend`, `Align`, etc. were taking >10 mins for
  * larger case classes. It's possible caching could fix this: https://youtu.be/80h3hZidSeE?t=34m7s
  *
  * It's also possible there was a recursive converter that didn't "LowerPriority" the implicits methods correctly.
  * https://stackoverflow.com/questions/1886953/is-there-a-way-to-control-which-implicit-conversion-will-be-the-default-used#answer-1887678
  *
  * If attempting to use the hlist ops, try them out on a test set of large case classes first before investing time in
  * building a full implementation.
  *
  * https://github.com/milessabin/shapeless/tree/shapeless-2.3.2#try-shapeless-with-an-ammonite-instant-repl
  *
  * @tparam From Class to convert from.
  * @tparam To   Class to convert to.
  * @see [[CaseClassTypeConverter]]
  */
@implicitNotFound(
  "Type ${From} and type ${To} do not have the same fields with the same names and same types."
)
trait SameFieldsTypeConverter[From, To] extends TypeConverter[From, To]

/**
  * Provides implicit SameFieldsConverter instances that may be used to convert between classes with the same fields.
  */
object SameFieldsTypeConverter {

  /**
    * Summons any implicitly available converter between types From and To.
    *
    * https://github.com/underscoreio/shapeless-guide/blob/0.3/src/pages/generic/type-classes.md#idiomatic-type-class-definitions-secgenericidiomatic-style
    *
    * @param sameFieldsConverter The implicit converter.
    * @tparam From Class to convert from.
    * @tparam To   Class to convert to.
    * @return The summoned converter.
    */
  def apply[From, To](
      implicit sameFieldsConverter: SameFieldsTypeConverter[From, To]
  ): TypeConverter[From, To] = {
    sameFieldsConverter
  }

  /**
    * Creates a converter for any types that have the same field representation.
    *
    * @param fromGen Converts an instance of From to a generic representation SameRepr.
    * @param toGen   Converts an instance of To from a generic representation SameRepr.
    * @tparam From     Class to convert from.
    * @tparam To       Class to convert to.
    * @tparam SameRepr The common representation of From and To.
    * @return A converter between From and To.
    */
  implicit def genericConverter[From, To, SameRepr <: HList](
      implicit
      fromGen: LabelledGeneric.Aux[From, SameRepr],
      toGen: LabelledGeneric.Aux[To, SameRepr]
  ): SameFieldsTypeConverter[From, To] = { (from: From) =>
    val hlist = fromGen.to(from)
    val to = toGen.from(hlist)
    to
  }
}
