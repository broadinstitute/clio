package org.broadinstitute.clio.util.json

import io.circe.{Decoder, Json}

trait DecodingUtil extends ModelAutoDerivation {

  def getByName[A: Decoder](json: Json, name: String): A = {
    json.hcursor.get[A](name).fold(throw _, identity)
  }
}
