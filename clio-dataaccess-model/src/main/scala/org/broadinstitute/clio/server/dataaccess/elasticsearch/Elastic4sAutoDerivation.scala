package org.broadinstitute.clio.server.dataaccess.elasticsearch

import io.circe.Json
import org.broadinstitute.clio.util.json.ModelAutoDerivation

/**
  * Automatic JSON encoding and decoding for use with elastic4s-circe.
  *
  * elastic4s's `Indexable` typeclass serves a similar purpose to circe's `Encoder`,
  * but instead of converting objects to `Json` it converts them to `String`.
  * elastic4s-circe defines a bridge method between the two which, given an implicit
  * `Encoder` for an object and an implicit "printer" to convert `Json => String`,
  * will build an `Indexable` that calls the printer on the output of the `Encoder`.
  *
  * `ModelAutoDerivation` provides the implicit `Encoder`, and this trait provides
  * the implicit `Json => String`.
  *
  * https://github.com/sksamuel/elastic4s/blob/v5.4.5/elastic4s-circe/src/main/scala/com/sksamuel/elastic4s/circe/package.scala#L41
  */
trait Elastic4sAutoDerivation extends ModelAutoDerivation {

  import scala.language.implicitConversions

  /**
    * Use our default printer, which formats the webservice outputs with no spaces,
    * as Elasticsearch bulk operations separate the json content with newlines.
    *
    * https://www.elastic.co/guide/en/elasticsearch/reference/5.4/docs-bulk.html
    */
  implicit def implicitEncoder(json: Json): String =
    defaultPrinter.pretty(json)
}
