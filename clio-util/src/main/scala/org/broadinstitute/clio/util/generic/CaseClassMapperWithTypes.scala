package org.broadinstitute.clio.util.generic

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.Type

/**
  * Creates an extension of a class mapper with the scala reflection Types also available.
  *
  * @tparam T The type of the case class, with a context bound also specifying that both an `implicit ctag: ClassTag[T]`
  *           exists, plus an `implicit fieldMapper: FieldMapper[T]` exists.
  *           https://www.scala-lang.org/files/archive/spec/2.12/07-implicits.html#context-bounds-and-view-bounds
  * @see [[FieldMapper]]
  */
class CaseClassMapperWithTypes[T: ClassTag](implicit mapper: FieldMapper[T])
    extends CaseClassMapper[T] {

  final def typeOf(field: String): Type = mapper.typeOf(field)
}
