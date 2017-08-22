package org.broadinstitute.clio.util.json

import org.broadinstitute.clio.transfer.model.TransferWgsUbamV1QueryOutput

import io.circe.Json

object JsonSchemas {
  val WgsUbam: Json = JsonSchema[TransferWgsUbamV1QueryOutput].toJson
}
