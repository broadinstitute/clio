package org.broadinstitute.clio.server.dataaccess.elasticsearch

import io.circe.Json
import org.broadinstitute.clio.util.json.ModelAutoDerivation

/** Automatic JSON encoding and decoding for the Elasticsearch DAO. */
trait Elastic4sAutoDerivation extends ModelAutoDerivation {

  import scala.language.implicitConversions

  /**
    * Formats the webservice outputs with no spaces, as Elasticsearch bulk operations separate the json content with
    * newlines.
    *
    * https://www.elastic.co/guide/en/elasticsearch/reference/5.4/docs-bulk.html
    *
    * https://github.com/sksamuel/elastic4s/blob/v5.4.5/elastic4s-circe/src/main/scala/com/sksamuel/elastic4s/circe/package.scala#L41
    */
  implicit def implicitEncoder(json: Json): String =
    defaultPrinter.pretty(json)
}

object Elastic4sAutoDerivation extends Elastic4sAutoDerivation
