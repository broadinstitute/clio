package org.broadinstitute.clio.util.generic

import scala.reflect.{ClassTag, classTag}

import java.lang.reflect.{Constructor, Field, Method}

/**
  * Facilitates extracting fields from a case class, and creating new instances of a case class from a map.
  *
  * Case classes must not be inner classes within another trait or class.
  *
  * If any field names are not recognized by newInstance or copy, an exception is thrown.
  *
  * Based on this stack overflow comment:
  * [[https://stackoverflow.com/questions/17312254/scala-case-class-copy-with-dynamic-named-parameter#comment64485427_23644859]]
  *
  * Other possibilities:
  *
  * [[https://github.com/bfil/scala-automapper Scala AutoMapper]]
  * - Uses custom macros, not shapeless.
  * - May be viable for compile time checking.
  * - Yet another library to learn.
  * - More dynamic class combining may be difficult, for example concatenating two types.
  *
  * [[https://github.com/kailuowang/henkan Henkan]]
  * - Uses shapeless.
  * - May (would?) have the same problem with large case classes.
  *
  * @tparam T The type of the case class, with a context bound also specifying that a `implicit ctag: ClassTag[T]`
  *           exists.
  *           https://www.scala-lang.org/files/archive/spec/2.12/07-implicits.html#context-bounds-and-view-bounds
  * @see [[SameFieldsTypeConverter]]
  */
class CaseClassMapper[T: ClassTag] {

  /** The field names. */
  lazy val names: Seq[String] = fields.map(_.getName)

  /** Returns a Map of field names to values from an instance of T. */
  def vals(t: T): Map[String, _] =
    getterMap.values.map(method => method.getName -> method.invoke(t)).toMap

  /** Creates an instance of T from a Map of field names to values. */
  def newInstance(vals: Map[String, _]): T = {

    /* When there is no value for a field, default the value as None, or throw an error if the type isn't Option. */
    def defaultNone(name: String, tpe: Class[_]): None.type = {
      if (!tpe.isAssignableFrom(classOf[Option[_]]))
        throw new IllegalArgumentException(
          s"Missing field $name: ${tpe.getName} for ${clazz.getName}"
        )
      None
    }

    newInstance(vals, defaultNone)
  }

  /** Creates a copy of the original, overwriting fields with the passed in vals. */
  def copy(original: T, vals: Map[String, _]): T = {
    newInstance(vals, (name, _) => getterMap(name).invoke(original))
  }

  /**
    * Creates a new instance of T.
    *
    * @param vals         The vals to use for creating an instance.
    * @param defaultValue Retrieves a value for some field name and type if a field value isn't in vals.
    * @return The new instance of T.
    */
  private def newInstance(
    vals: Map[String, _],
    defaultValue: (String, Class[_]) => Any
  ): T = {
    val unknownVals = vals.filterKeys(key => !names.contains(key))
    if (unknownVals.nonEmpty)
      throw new IllegalArgumentException(
        s"Unknown val for ${clazz.getName}:\n  ${unknownVals.mkString("\n  ")}"
      )

    val args = names map { name =>
      val tpe = typeMap(name)
      vals.get(name) match {
        case Some(value) if value == null => value
        case Some(value) =>
          if (!tpe.isAssignableFrom(value.getClass)) {
            throw new IllegalArgumentException(
              s"Value '$value' of type ${value.getClass.getName} cannot be assigned to field $name: ${tpe.getName}"
            )
          }
          value
        case None =>
          defaultValue(name, tpe)
      }
    }
    constructor.newInstance(args.asInstanceOf[Seq[AnyRef]]: _*)
  }

  /** The java.lang.Class of the case class. */
  private val clazz: Class[T] = classTag[T].runtimeClass.asInstanceOf[Class[T]]

  /** The fields within the case class. */
  private val fields: Seq[Field] = clazz.getDeclaredFields

  /** A constructor that takes all the fields. */
  private val constructor: Constructor[T] =
    clazz.getConstructor(fields.map(_.getType): _*)

  /** Utility for creating a map based on the fields. */
  private def fieldMapping[V](map: Field => V): Map[String, V] =
    fields.map(field => field.getName -> map(field)).toMap

  /** Getters for each field. */
  private val getterMap: Map[String, Method] = fieldMapping(
    field => clazz.getMethod(field.getName)
  )

  /** Class for each field. */
  private val typeMap: Map[String, Class[_]] = fieldMapping(
    field => CaseClassMapper.primitiveWrapper(field.getType)
  )
}

object CaseClassMapper {

  /** Unboxes primitive types, or returns the passed in clazz. */
  private def primitiveWrapper(clazz: Class[_]): Class[_] =
    primitiveWrappers.getOrElse(clazz, clazz)

  /** Maps scala primitive types to their boxed types. */
  private val primitiveWrappers: Map[Class[_], Class[_]] = Map(
    classOf[Boolean] -> classOf[java.lang.Boolean],
    classOf[Byte] -> classOf[java.lang.Byte],
    classOf[Char] -> classOf[java.lang.Character],
    classOf[Double] -> classOf[java.lang.Double],
    classOf[Float] -> classOf[java.lang.Float],
    classOf[Int] -> classOf[java.lang.Integer],
    classOf[Long] -> classOf[java.lang.Long],
    classOf[Short] -> classOf[java.lang.Short]
  )

}
