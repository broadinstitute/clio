package org.broadinstitute.clio.util.generic

import scala.reflect.ClassTag

/**
  * Convert from one case class to another.
  *
  * @param convertVals Function to modify the the extracted from the From before they are used to initiate an instance
  *                    of To.
  * @tparam From The type of the original case class, with a context bound also specifying that an
  *              `implicit ctagFrom: ClassTag[T]` exists.
  *              https://www.scala-lang.org/files/archive/spec/2.12/07-implicits.html#context-bounds-and-view-bounds
  * @tparam To   The type of the destination case class, with a context bound also specifying that an
  *              `implicit ctagTo: ClassTag[T]` exists.
  *              https://www.scala-lang.org/files/archive/spec/2.12/07-implicits.html#context-bounds-and-view-bounds
  * @see [[SameFieldsTypeConverter]]
  */
class CaseClassTypeConverter[From: ClassTag, To: ClassTag] private (
    convertVals: Map[String, _] => Map[String, _]
) extends TypeConverter[From, To] {
  private val fromMapper = new CaseClassMapper[From]
  private val toMapper = new CaseClassMapper[To]

  override def convert(from: From): To = {
    val fromVals = fromMapper.vals(from)
    val modifiedVals = convertVals(fromVals)
    val to = toMapper.newInstance(modifiedVals)
    to
  }
}

object CaseClassTypeConverter {

  /**
    * Create an instance of a CaseClassTypeConverter.
    *
    * @param convertVals Function to modify the the extracted from the From before they are used to initiate an instance
    *                    of To.
    * @tparam From The type of the original case class, with a context bound also specifying that an
    *              `implicit ctagFrom: ClassTag[T]` exists.
    *              https://www.scala-lang.org/files/archive/spec/2.12/07-implicits.html#context-bounds-and-view-bounds
    * @tparam To   The type of the destination case class, with a context bound also specifying that an
    *              `implicit ctagTo: ClassTag[T]` exists.
    *              https://www.scala-lang.org/files/archive/spec/2.12/07-implicits.html#context-bounds-and-view-bounds
    * @return An instance of a CaseClassTypeConverter.
    */
  def apply[From: ClassTag, To: ClassTag](
      convertVals: Map[String, _] => Map[String, _]
  ): TypeConverter[From, To] = new CaseClassTypeConverter(convertVals)
}
