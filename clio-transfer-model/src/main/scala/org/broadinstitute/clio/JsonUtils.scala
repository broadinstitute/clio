package org.broadinstitute.clio

import io.circe.{Decoder, Json}

object JsonUtils {

  /**
    * Wrapper for circe's [[Json]], used to add common patterns as extension methods.
    */
  implicit class JsonOps(val json: Json) extends AnyVal {

    /**
      * Assume this [[Json]] is an object, and try to extract the value of the given
      * name, decoded as the given type. Throws any errors that circe might raise in
      * the process.
      */
    def unsafeGet[T: Decoder](key: String): T =
      json.hcursor.get[T](key).fold(throw _, identity)

    def dropNulls: Json = {
      json.mapObject(_.filter {
        case (_, v) => !v.isNull
      })
    }
  }
}
