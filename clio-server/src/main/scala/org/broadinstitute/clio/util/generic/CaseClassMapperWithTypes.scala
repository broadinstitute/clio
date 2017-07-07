package org.broadinstitute.clio.util.generic

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.Type

/**
  * Creates an extension of a class mapper with the scala reflection Types also available.
  *
  * @tparam T The type of the case class, with a context bound also specifying that both an `implicit ctag: ClassTag[T]`
  *           exists, plus an `implicit fieldMapper: FieldMapper[T]` exists.
  *           https://www.scala-lang.org/files/archive/spec/2.12/07-implicits.html#context-bounds-and-view-bounds
  * @see [[org.broadinstitute.clio.util.generic.FieldMapper]]
  */
class CaseClassMapperWithTypes[T: ClassTag: FieldMapper]
    extends CaseClassMapper[T] {

  /** The scala reflection types for each field. */
  val types: Map[String, Type] = FieldMapper[T].fields
}
