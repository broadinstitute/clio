package org.broadinstitute.clio.util.generic

import scala.reflect.{ClassTag, classTag}

/**
  * For a set of classes, stores some cached value based on the companion of those instances.
  */
class CompanionCache {
  private var companionCache: Map[Class[_], _] = Map.empty

  /**
    * Gets or caches a value: S associated with the companion: C of type T.
    *
    * @param build A converter from the companion
    * @tparam T original type, with a context bound also specifying that an `implicit ctag: ClassTag[T]` exists.
    *           https://www.scala-lang.org/files/archive/spec/2.12/07-implicits.html#context-bounds-and-view-bounds
    * @tparam C companion type
    * @tparam S stored type
    * @return The cached value, or the value returned from build.
    */
  def cached[T: ClassTag, C, S](build: C => S): S = {
    val clazz = classTag[T].runtimeClass
    companionCache.get(clazz) match {
      case Some(result) => result.asInstanceOf[S]
      case None =>
        val companion = CompanionCache.getCompanion(clazz)
        val result = build(companion.asInstanceOf[C])
        companionCache += clazz -> result
        result
    }
  }
}

object CompanionCache {

  /**
    * Returns the companion instances for clazz using java reflection.
    *
    * Scala reflection was... verbose:
    *
    * [[https://stackoverflow.com/questions/36068089/what-is-a-reliable-way-to-find-a-scala-types-companion-object-via-java-reflecti java]]
    *
    * - vs. -
    *
    * [[https://stackoverflow.com/questions/11020746/get-companion-object-instance-with-new-scala-reflection-api/11031443#11031443 scala]]
    *
    * @param clazz The class or trait to retrieve the companion for.
    * @return The companion instance.
    */
  private def getCompanion(clazz: Class[_]): AnyRef = {
    val companion = Class.forName(clazz.getName + "$")
    companion.getField("MODULE$").get(companion)
  }
}
